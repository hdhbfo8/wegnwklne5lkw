import telebot
import gspread
from gspread.exceptions import WorksheetNotFound
import pandas as pd
from datetime import datetime
import pytz
import re
from apscheduler.schedulers.background import BackgroundScheduler
import time

# =================================================================
# 1. КОНСТАНТЫ И НАСТРОЙКИ
# =================================================================

# 1.1. Токен вашего бота
TELEGRAM_TOKEN = "8393166216:AAFqm9AcgzuK5Ck7rWUHjrtd5L3VZ5FMkyQ"

# 1.2. ID вашей Google Таблицы
SHEET_ID = "1lZbDSPMI_ifK7T0f2gYCHt2Ci7JbJ0KLfGhtJGNyvLI"

# 1.3. Имя файла ключа Service Account (JSON файл)
SERVICE_ACCOUNT_FILE = 'key.json'

# 1.4. Имя рабочего листа в таблице
WORKSHEET_NAME = 'Лист1'

# 1.5. Ваш Chat ID для отправки уведомлений
CHAT_ID_FOR_NOTIFICATIONS = 545995109

# 1.6. Настройки времени (Екатеринбург UTC+5)
TIMEZONE = pytz.timezone('Asia/Yekaterinburg')
NOTIFICATION_HOUR = 19  # 19:00 по ЕКБ

# Инициализация бота
bot = telebot.TeleBot(TELEGRAM_TOKEN)

# =================================================================
# 2. ФУНКЦИИ GOOGLE SHEETS И РАСЧЕТ ДАТЫ
# =================================================================

def connect_to_sheet():
    """Подключение к Google Таблице с использованием Service Account."""
    try:
        gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
        return gc.open_by_key(SHEET_ID).worksheet(WORKSHEET_NAME)
    except FileNotFoundError:
        print(f"Ошибка: Не найден файл ключа '{SERVICE_ACCOUNT_FILE}'. Убедитесь, что он лежит рядом с ботом.")
        return None
    except WorksheetNotFound:
        print(f"Ошибка: Не найден рабочий лист '{WORKSHEET_NAME}'. Проверьте его название.")
        return None
    except Exception as e:
        print(f"Ошибка подключения к Google Sheets: {e}")
        return None

def calculate_payout_date(date_str):
    """
    Рассчитывает дату выплаты по правилам Авито (7, 14, 21, 28 числа).
    Ввод: Строка с датой получения (например, '28.11').
    Вывод: Строка с датой выплаты ('DD.MM.YYYY').
    """
    try:
        # 1. Добавляем текущий год для корректного парсинга
        current_year = datetime.now(TIMEZONE).year
        full_date_str = f"{date_str}.{current_year}"
        date_received = datetime.strptime(full_date_str, '%d.%m.%Y')

        day = date_received.day
        year = date_received.year
        month = date_received.month

        # Логика расчета даты
        if day <= 7:
            payout_day = 14
        elif day <= 14:
            payout_day = 21
        elif day <= 21:
            payout_day = 28
        else:  # day > 21
            # Переход на следующее 7 число следующего месяца
            month += 1
            payout_day = 7
        
        # Обработка перехода года
        if month > 12:
            month = 1
            year += 1

        payout_date = datetime(year, month, payout_day)
        return payout_date.strftime('%d.%m.%Y')
        
    except Exception as e:
        print(f"Ошибка расчета даты выплаты: {e}")
        return "Ошибка"

def get_sheet_data():
    """Чтение данных из таблицы с использованием pandas."""
    sheet = connect_to_sheet()
    if not sheet:
        return pd.DataFrame(), None 
    
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    
    # Конвертация типов данных
    if not df.empty:
        df['summa'] = pd.to_numeric(df['summa'], errors='coerce').fillna(0)
        # dayfirst=True важен для формата DD.MM.YYYY
        df['data_vyplaty'] = pd.to_datetime(df['data_vyplaty'], errors='coerce', dayfirst=True)
    
    return df, sheet

# =================================================================
# 3. ОСНОВНАЯ ЛОГИКА БОТА
# =================================================================

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    """Обработка команд /start и /help."""
    markup = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True)
    btn1 = telebot.types.KeyboardButton("📈 План выплат")
    markup.add(btn1)
    
    response = (
        "Привет! Я твой финансовый помощник по Авито.\n\n"
        "**Как вносить данные:**\n"
        "1. **Новый заказ (Отправка):** `Новый 4521 5000` (ID заказа + Сумма)\n"
        "2. **Заказ завершен (Получение):** `Забрал 4521 27.11` (ID заказа + Дата получения)\n\n"
        "Я теперь сам рассчитываю дату выплаты!"
    )
    bot.reply_to(message, response, parse_mode='Markdown', reply_markup=markup)


@bot.message_handler(regexp=r'^Новый (\d+) (\d+)$')
def handle_new_order(message):
    """Обработка ввода нового заказа: Новый 4521 5000."""
    match = re.match(r'^Новый (\d+) (\d+)$', message.text)
    if not match:
        return
        
    order_id = match.group(1)
    amount = match.group(2)
    current_date = datetime.now(TIMEZONE).strftime('%d.%m.%Y')
    
    sheet = connect_to_sheet()
    if not sheet:
        bot.reply_to(message, "Ошибка: Не удалось подключиться к Google Таблице.")
        return

    try:
        # Ищем, нет ли уже такого ID
        if sheet.find(order_id, in_column=1):
            bot.reply_to(message, f"❌ Ошибка: Заказ с ID **{order_id}** уже есть в таблице! Используйте другой ID или команду 'Забрал'.", parse_mode='Markdown')
            return
            
        # Добавляем новую строку (A, B, C, D). Столбец E оставляем чистым.
        sheet.append_row([order_id, current_date, amount, ''])
        bot.reply_to(message, f"✅ Заказ **№{order_id}** на **{amount} ₽** записан.\nСтатус: Отправлен ({current_date}).", parse_mode='Markdown')
        
    except Exception as e:
        bot.reply_to(message, f"Произошла ошибка при записи в таблицу: {e}")


@bot.message_handler(regexp=r'^Забрал (\d+) (\d{1,2}\.\d{1,2})$')
def handle_order_received(message):
    """Обработка завершения заказа: Забрал 4521 27.11."""
    match = re.match(r'^Забрал (\d+) (\d{1,2}\.\d{1,2})$', message.text)
    if not match:
        return
        
    order_id = match.group(1)
    date_str = match.group(2)
    
    sheet = connect_to_sheet()
    if not sheet:
        bot.reply_to(message, "Ошибка: Не удалось подключиться к Google Таблице.")
        return

    try:
        # Ищем заказ по ID в первом столбце
        cell = sheet.find(order_id, in_column=1)
        if not cell:
            bot.reply_to(message, f"❌ Ошибка: Заказ с ID **{order_id}** не найден в таблице.", parse_mode='Markdown')
            return
            
        # Обновляем столбец D (data_polucheniya)
        sheet.update_cell(cell.row, 4, date_str) 
        
        # 1. Рассчитываем дату выплаты
        payout_date_str = calculate_payout_date(date_str)
        
        # 2. Записываем рассчитанную дату в столбец E (Индекс 5)
        sheet.update_cell(cell.row, 5, payout_date_str)
        
        response = (
            f"✅ Статус заказа **№{order_id}** обновлен:\n"
            f"   Дата получения: **{date_str}**\n"
            f"   Ожидаемая выплата: **{payout_date_str}**"
        )
        bot.reply_to(message, response, parse_mode='Markdown')
        
    except Exception as e:
        bot.reply_to(message, f"Произошла ошибка при обновлении таблицы: {e}")


@bot.message_handler(regexp='📈 План выплат|/plan')
def get_payment_plan(message):
    """Обработка кнопки/команды для просмотра плана выплат."""
    df, _ = get_sheet_data()
    
    if df.empty:
        bot.reply_to(message, "Таблица пуста или ошибка подключения.")
        return

    # --- ИСПРАВЛЕНО ЗДЕСЬ ---
    # Создаем Timestamp для текущей даты (без времени), чтобы типы данных совпадали
    today_ts = pd.Timestamp(datetime.now(TIMEZONE).date())

    # Фильтруем будущие даты
    df_future = df[
        (df['data_vyplaty'] >= today_ts) &
        (df['data_vyplaty'].notna()) 
    ].copy()

    if df_future.empty:
        bot.reply_to(message, "На ближайшее время ожидаемых выплат нет.")
        return
        
    # Группируем по дате выплаты и суммируем
    payout_summary = df_future.groupby('data_vyplaty')['summa'].sum().reset_index()
    payout_summary = payout_summary.sort_values(by='data_vyplaty')
    
    # Берем 3 ближайшие даты
    top_3_payouts = payout_summary.head(3)
    
    output = "💰 **ПЛАН ВЫПЛАТ АВИТО (3 ближайших периода):**\n\n"
    for index, row in top_3_payouts.iterrows():
        # Приводим дату к строке для вывода
        date_str = row['data_vyplaty'].strftime('%d %B')
        amount = int(row['summa'])
        output += f"🗓️ **{date_str}**: **{amount:,.0f} ₽**\n" 
        
    bot.reply_to(message, output, parse_mode='Markdown')


# =================================================================
# 4. ФУНКЦИЯ УВЕДОМЛЕНИЙ (ПЛАНИРОВЩИК)
# =================================================================

def send_daily_notification():
    """Проверяет, есть ли сегодня выплаты, и отправляет уведомление."""
    df, _ = get_sheet_data()
    
    if df.empty:
        return

    # --- ИСПРАВЛЕНО ЗДЕСЬ ---
    # Создаем Timestamp для сравнения
    today_ts = pd.Timestamp(datetime.now(TIMEZONE).date())
    
    # Ищем выплаты, назначенные на сегодня
    df_today = df[
        (df['data_vyplaty'] == today_ts) &
        (df['data_vyplaty'].notna())
    ]
    total_amount = df_today['summa'].sum()

    if total_amount > 0:
        date_str_display = today_ts.strftime('%d.%m')
        message_text = (
            f"🔔 **НАПОМИНАНИЕ О ВЫПЛАТЕ!**\n\n"
            f"Сегодня ({date_str_display}) по графику Авито ожидается поступление на сумму: **{int(total_amount):,.0f} ₽**.\n\n"
            f"Пожалуйста, проверьте ваш расчетный счет."
        )
        try:
            bot.send_message(CHAT_ID_FOR_NOTIFICATIONS, message_text, parse_mode='Markdown')
            print(f"Отправлено уведомление о выплате {total_amount} ₽.")
        except Exception as e:
            print(f"Ошибка отправки уведомления для CHAT_ID {CHAT_ID_FOR_NOTIFICATIONS}: {e}")

# =================================================================
# 5. ЗАПУСК БОТА И ПЛАНИРОВЩИКА
# =================================================================

if __name__ == '__main__':
    # 5.1. Настройка и запуск планировщика
    scheduler = BackgroundScheduler(timezone=TIMEZONE)
    # Запускаем проверку каждый день в указанный час
    scheduler.add_job(send_daily_notification, 'cron', hour=NOTIFICATION_HOUR)
    scheduler.start()
    print(f"Планировщик запущен. Ежедневное уведомление настроено на {NOTIFICATION_HOUR}:00 по ЕКБ.")
    
    # 5.2. Запуск бота
    print("Бот запущен. Нажмите Ctrl+C для остановки.")
    try:
        bot.polling(none_stop=True)
    except Exception as e:
        print(f"Бот остановлен из-за ошибки: {e}")
    finally:
        # Останавливаем планировщик при завершении работы бота
        scheduler.shutdown()
        