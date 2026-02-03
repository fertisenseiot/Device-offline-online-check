import os
import pymysql
import requests
import pytz
from datetime import datetime, timedelta
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException
from twilio.rest import Client
from datetime import time as dt_time, timedelta

def safe_time(t):
    """
    Converts MySQL TIME / timedelta / datetime.time safely to datetime.time
    """
    if t is None:
        return dt_time(0, 0, 0)

    if isinstance(t, dt_time):
        return t

    if isinstance(t, timedelta):
        total_seconds = int(t.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return dt_time(hours % 24, minutes, seconds)

    # fallback
    try:
        return t.time()
    except Exception:
        return dt_time(0, 0, 0)



IST = pytz.timezone("Asia/Kolkata")

# 🔥 DEBUG START (YAHI LAGANA HAI)
print("🚀 Device Online/Offline Script Started")

# =====================================================
# DATABASE CONFIG
# =====================================================
db_config = {
    "host": "switchyard.proxy.rlwy.net",
    "user": "root",
    "port": 28085,
    "password": "NOtYUNawwodSrBfGubHhwKaFtWyGXQct",
    "database": "railway",
    "cursorclass": pymysql.cursors.DictCursor
}

# =====================================================
# SMS CONFIG (Universal SMS)
# =====================================================
SMS_API_URL = "https://www.universalsmsadvertising.com/universalsmsapi.php"
SMS_USER = "8960853914"
SMS_PASS = "8960853914"
SENDER_ID = "FRTLLP"

# =====================================================
# SMS FOR NOTIFICATION
# =====================================================
def build_message(ntf_typ, devnm):
    messages = {
        3: f"WARNING!! The lab device {devnm} is offline. Please take necessary action- Regards Fertisense LLP",
        # 5: f"INFO!! The device {devnm} is back online. No action is required - Regards Fertisense LLP",
    }
    return messages.get(ntf_typ, f"Alert for {devnm} - Regards Fertisense LLP")


# =====================================================
# BREVO EMAIL CONFIG
# =====================================================
BREVO_API_KEY = os.getenv("BREVO_API_KEY")
print("🔑 BREVO_API_KEY FOUND:", bool(BREVO_API_KEY))
BREVO_SENDER_EMAIL = "fertisenseiot@gmail.com"   # VERIFIED
BREVO_SENDER_NAME = "Fertisense LLP"


# =====================================================
# SMS CONFIG (Universal SMS)
# =====================================================
TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN")
TWILIO_NUMBER = os.getenv("TWILIO_NUMBER")

twilio = Client(TWILIO_SID, TWILIO_TOKEN)


# =====================================================
# SMS FUNCTION
# =====================================================
def send_sms(message, mobile):
    try:
        mobile = str(mobile).strip()

        payload = {
            "user_name": SMS_USER,
            "user_password": SMS_PASS,
            "mobile": mobile,
            "sender_id": SENDER_ID,
            "type": "F",
            "text": message
        }

        r = requests.get(SMS_API_URL, params=payload, timeout=20)
        print("📱 SMS API:", r.status_code, r.text)

        return r.status_code == 200

    except Exception as e:
        print("❌ SMS Error:", e)
        return False

    # for sending multiple emails 
def extract_unique_emails(email_list):
    result = set()
    for e in email_list:
        if e:
            parts = e.split(",")
            for p in parts:
                p = p.strip()
                if p:
                    result.add(p)
    return list(result)


# =====================================================
# EMAIL FUNCTION (BREVO SDK)
# =====================================================
def send_email(subject, message, to_email):
    try:
        config = sib_api_v3_sdk.Configuration()
        config.api_key["api-key"] = BREVO_API_KEY

        api_instance = sib_api_v3_sdk.TransactionalEmailsApi(
            sib_api_v3_sdk.ApiClient(config)
        )

        email = sib_api_v3_sdk.SendSmtpEmail(
            sender={
                "name": BREVO_SENDER_NAME,
                "email": BREVO_SENDER_EMAIL
            },
            to=[{"email": to_email}],
            subject=subject,
            html_content=f"""
                <html>
                    <body>
                        <p>{message}</p>
                        <br>
                        <p>Regards,<br><b>Fertisense LLP</b></p>
                    </body>
                </html>
            """
        )

        # api_instance.send_transac_email(email)

        response = api_instance.send_transac_email(email)
        print(f"📧 Email sent successfully to {to_email}")
        print("Brevo Message ID:", response.message_id)


    except ApiException as e:
        print("Email Error:", e)

# =====================================================
# TWILLIO CALL SPEC
# =====================================================
def normalize_phone(num):
    num = str(num).strip()
    if num.startswith("+"):
        return num
    return "+91" + num


def make_robo_call(phone, message):
    try:
        call = twilio.calls.create(
            to=phone,
            from_=TWILIO_NUMBER,
            twiml=f"<Response><Say voice='alice' language='en-IN'>{message}</Say></Response>",
            timeout=60,
            status_callback="https://fertisense-iot-production.up.railway.app/twilio/call-status/",
            status_callback_event=["initiated","answered","completed","busy","no-answer","failed"]
        )
        return call.sid
    except Exception as e:
        print("❌ Call failed:", e)
        return None

def get_call_count(cursor, device_status_alarm_id, phone):
    cursor.execute("""
        SELECT COUNT(*) cnt
        FROM iot_api_devicealarmcalllog
        WHERE DEVICE_STATUS_ALARM_ID=%s 
          AND PHONE_NUM=%s
          AND CALL_STATUS IN (0,2,3)
    """, (device_status_alarm_id, phone))
    return cursor.fetchone()["cnt"]


def is_alarm_answered(cursor, device_status_alarm_id):
    cursor.execute("""
        SELECT 1 FROM iot_api_devicealarmcalllog
        WHERE DEVICE_STATUS_ALARM_ID=%s 
            AND CALL_STATUS=1
        LIMIT 1
    """, (device_status_alarm_id,))
    return cursor.fetchone() is not None

# =====================================================
# FETCH USERS FOR ORG + CENTRE
# =====================================================
def get_alert_users(cursor, organization_id, centre_id):
    cursor.execute("""
        SELECT 
            u.USER_ID,
            u.PHONE,
            u.EMAIL,
            u.SEND_SMS,
            u.SEND_EMAIL
        FROM master_user u
        JOIN userorganizationcentrelink l
            ON l.USER_ID_id = u.USER_ID
        WHERE l.ORGANIZATION_ID_id = %s
          AND l.CENTRE_ID_id = %s
    """, (organization_id, centre_id))

    return cursor.fetchall()

# =====================================================
# CORE LOGIC
# =====================================================
def check_device_online_offline():
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()

    # now = datetime.now()
    # five_min_ago = now - timedelta(minutes=5)
    # cursor.execute("SELECT NOW() as db_now")
    # db_now = cursor.fetchone()["db_now"]
    now = datetime.now(IST)
    five_min_ago = now - timedelta(minutes=5)


    # All devices
    cursor.execute("""SELECT DEVICE_ID, DEVICE_NAME, ORGANIZATION_ID, CENTRE_ID
    FROM iot_api_masterdevice
    WHERE DEVICE_STATUS = 1
""")
    devices = cursor.fetchall()

    for d in devices:
        device_id = d["DEVICE_ID"]

        print("\n🔍 Checking DEVICE_ID:", device_id)

        # Last reading
        cursor.execute("""
            SELECT CONCAT(READING_DATE,' ',READING_TIME) AS last_time
            FROM device_reading_log
            WHERE DEVICE_ID = %s
            ORDER BY READING_DATE DESC, READING_TIME DESC
            LIMIT 1
        """, (device_id,))
        last = cursor.fetchone()

        if not last:
            continue
         
        try:
            last_time = datetime.strptime(
                str(last["last_time"]), "%Y-%m-%d %H:%M:%S.%f"
            )
        except ValueError:
            last_time = datetime.strptime(
                str(last["last_time"]), "%Y-%m-%d %H:%M:%S"
            )

        # 🔑 FINAL FIX — make last_time timezone-aware
        if last_time.tzinfo is None:
           last_time = IST.localize(last_time)

        is_online = last_time >= five_min_ago
        print("🕒 Last Reading Time:", last_time)
        print("⏱ 5 Min Threshold:", five_min_ago)
        print("📡 Is Online:", is_online)

        # Device org + centre
        # cursor.execute("""
        #     SELECT DEVICE_NAME, ORGANIZATION_ID, CENTRE_ID
        #     FROM iot_api_masterdevice
        #     WHERE DEVICE_ID = %s
        # """, (device_id,))
        # device_info = cursor.fetchone()

        # if not device_info:
        #     continue
        # device_name = device_info["DEVICE_NAME"]
        # organization_id = device_info["ORGANIZATION_ID"]
        # centre_id = device_info["CENTRE_ID"]

        device_name = d["DEVICE_NAME"]
        organization_id = d["ORGANIZATION_ID"]
        centre_id = d["CENTRE_ID"]


        # Previous status
        cursor.execute("""
           SELECT IS_ACTIVE
           FROM device_status_alarm_log
           WHERE DEVICE_ID = %s
           ORDER BY DEVICE_STATUS_ALARM_ID DESC
           LIMIT 1
        """, (device_id,))
        prev = cursor.fetchone()
        prev_is_active = prev["IS_ACTIVE"] if prev else 0
        print("📄 Previous Status:", prev_is_active)

        users = get_alert_users(cursor, organization_id, centre_id)
        print("👥 Users Found:", len(users))



        # ================= OFFLINE =================
        if not is_online and prev_is_active != 1:
        # if not is_online:
            sms_sent = False
            email_sent = False

                # 🔒 purana status close
            cursor.execute("""
               UPDATE device_status_alarm_log
               SET IS_ACTIVE = 0
               WHERE DEVICE_ID = %s AND IS_ACTIVE = 1
            """, (device_id,))

            msg = build_message(3, device_name)
            
            phones = []
            emails = []

            for user in users:
                if user["SEND_SMS"] == 1 and user["PHONE"]:
                    phones.append(user["PHONE"])
                    # for num in str(user["PHONE"]).split(","):
                    #     if send_sms(msg, num):
                    #         sms_sent = True
                
                if user["SEND_EMAIL"] == 1 and user["EMAIL"]:
                     emails.append(user["EMAIL"])

            # 🔑 FLATTEN + DEDUPE
            flat_phones = []
            for p in phones:
                if p:
                   for part in p.split(","):
                       num = part.strip()
                       if num:
                          flat_phones.append(num)

            unique_phones = list(set(flat_phones))
            print("Unique phone numbers:", unique_phones)

            
            # 3️⃣ FLATTEN + DEDUPE EMAILS
            unique_emails = extract_unique_emails(emails)
            print("📧 Unique emails:", unique_emails)

            
            # 4️⃣ SEND SMS
            sms_sent = False
            for phone in unique_phones:
                if send_sms(msg, phone):
                    sms_sent = True

            # 5️⃣ SEND EMAIL
            email_sent = False
            for email in unique_emails:
                send_email("Device Offline Alert", msg, email)
                email_sent = True

                # if user["SEND_EMAIL"] == 1 and user["EMAIL"]:
                #     send_email("Device Offline Alert", msg, user["EMAIL"])
                #     email_sent = True

            cursor.execute("""
                INSERT INTO device_status_alarm_log
                (DEVICE_ID, DEVICE_STATUS, IS_ACTIVE, CREATED_ON_DATE, CREATED_ON_TIME, SMS_DATE, SMS_TIME, EMAIL_DATE, EMAIL_TIME)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                device_id,
                1,
                1,
                now.date(),
                now.time(),
                now.date() if sms_sent else None,
                now.time() if sms_sent else None,
                now.date() if email_sent else None,
                now.time() if email_sent else None
            ))
            conn.commit()

            # ================= ROBO CALL AFTER 5 MIN =================
        if not is_online and prev_is_active == 1:

                    # 👇👇👇 YAHI ADD KARNA HAI 👇👇👇
            msg = build_message(3, device_name)

            phones = []
            emails = []

            for user in users:
                if user["SEND_SMS"] == 1 and user["PHONE"]:
                    phones.append(user["PHONE"])
                if user["SEND_EMAIL"] == 1 and user["EMAIL"]:
                    emails.append(user["EMAIL"])

            flat_phones = []
            for p in phones:
                for part in p.split(","):
                    part = part.strip()
                    if part:
                        flat_phones.append(part)

            unique_phones = list(set(flat_phones))
            unique_emails = extract_unique_emails(emails)
            # 👆👆👆 YAHAN TAK 👆👆👆

            cursor.execute("""
                SELECT DEVICE_STATUS_ALARM_ID, SMS_DATE, SMS_TIME
                FROM device_status_alarm_log
                WHERE DEVICE_ID=%s AND IS_ACTIVE=1
                ORDER BY DEVICE_STATUS_ALARM_ID DESC
                LIMIT 1
            """, (device_id,))
            alarm = cursor.fetchone()

            if alarm and alarm["SMS_DATE"]:

                first_sms_dt = IST.localize(
                    datetime.combine(
                        alarm["SMS_DATE"],
                        safe_time(alarm["SMS_TIME"])
                    )
                )

                

                elapsed = (now - first_sms_dt).total_seconds()

                if elapsed >= 300:   # ⏱️ 5 minutes

                    if is_alarm_answered(cursor, alarm["DEVICE_STATUS_ALARM_ID"]):
                        print("🔕 Alarm already acknowledged")
                        continue

                    for phone in unique_phones:
                        phone = normalize_phone(phone)
                        attempts = get_call_count(
                            cursor,
                            alarm["DEVICE_STATUS_ALARM_ID"],
                            phone
                        )

                        if attempts >= 3:
                              continue

                        # voice_msg = (
                        #     f"WARNING! Device {device_name} is OFFLINE. "
                        #     "Immediate attention required."
                        # )

                        call_sid = make_robo_call(phone, msg)

                        if call_sid:
                           cursor.execute("""
                              INSERT INTO iot_api_devicealarmcalllog
                              (DEVICE_ID, DEVICE_STATUS_ALARM_ID, PHONE_NUM,
                               CALL_DATE, CALL_TIME, CALL_SID, SMS_CALL_FLAG, CALL_STATUS)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        """, (
                             device_id,
                             alarm["DEVICE_STATUS_ALARM_ID"],
                             phone,
                             now.date(),
                             now.time(),
                             call_sid,
                             1,  # 👈 SMS_CALL_FLAG (1 = call via SMS/robo)
                             0   # 👈 CALL_STATUS = PENDING
                        ))
                        conn.commit()
                        break   # 🔥 sirf EK call per cron


            # ================= SECOND NOTIFICATION =================
            elapsed_hours = (now - first_sms_dt).total_seconds() / 3600

            if elapsed_hours >= 6:

                # 📩 SMS (same msg)
                for phone in unique_phones:
                    send_sms(msg, phone)

                # 📧 EMAIL (same msg)
                for email in unique_emails:
                    send_email(
                        "2nd Offline Alert",
                        msg,
                        email
                    )

                cursor.execute("""
                    UPDATE device_status_alarm_log
                    SET EMAIL_DATE=%s, EMAIL_TIME=%s
                    WHERE DEVICE_STATUS_ALARM_ID=%s
                """, (now.date(), now.time(), alarm["DEVICE_STATUS_ALARM_ID"]))

                conn.commit()

                print("✅ Second notification sent (SMS + Email only)")


        # ================= ONLINE =================
        # elif is_online and prev_is_active == 1:
        # # elif is_online:
        #     sms_sent = False
        #     email_sent = False

        #     cursor.execute("""
        #         UPDATE device_status_alarm_log
        #         SET IS_ACTIVE = 0
        #         WHERE DEVICE_ID = %s AND IS_ACTIVE = 1
        #     """, (device_id,))

        #     # msg = f"INFO!! The {device_name} is back online. No action is required - Regards Fertisense LLP"
        #     msg = build_message(5, device_name)



        #     phones = []
        #     emails = []

        #     for user in users:
        #         if user["SEND_SMS"] == 1 and user["PHONE"]:
        #             phones.append(user["PHONE"])
        #             # for num in str(user["PHONE"]).split(","):
        #             #     if send_sms(msg, num):
        #             #         sms_sent = True
                
        #         if user["SEND_EMAIL"] == 1 and user["EMAIL"]:
        #              emails.append(user["EMAIL"])

        #     # 🔑 FLATTEN + DEDUPE
        #     flat_phones = []
        #     for p in phones:
        #         if p:
        #            for part in p.split(","):
        #                num = part.strip()
        #                if num:
        #                   flat_phones.append(num)

        #     unique_phones = list(set(flat_phones))
        #     print("Unique phone numbers:", unique_phones)

            
        #     # 3️⃣ FLATTEN + DEDUPE EMAILS
        #     unique_emails = list(set([e.strip() for e in emails if e.strip()]))
        #     print("📧 Unique emails:", unique_emails)

            
        #     # 4️⃣ SEND SMS
        #     sms_sent = False
        #     for phone in unique_phones:
        #         if send_sms(msg, phone):
        #             sms_sent = True

        #     # 5️⃣ SEND EMAIL
        #     email_sent = False
        #     for email in unique_emails:
        #         send_email("Device Online Info", msg, email)
        #         email_sent = True

        #     cursor.execute("""
        #         INSERT INTO device_status_alarm_log
        #         (DEVICE_ID, DEVICE_STATUS, IS_ACTIVE, CREATED_ON_DATE, CREATED_ON_TIME, SMS_DATE, SMS_TIME, EMAIL_DATE, EMAIL_TIME)
        #         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        #     """, (
        #         device_id,
        #         0,
        #         0,
        #         now.date(),
        #         now.time(),
        #         now.date() if sms_sent else None,
        #         now.time() if sms_sent else None,
        #         now.date() if email_sent else None,
        #         now.time() if email_sent else None
        #     ))
        #     conn.commit()

    # conn.close()


# =====================================================
# ENTRY POINT
# =====================================================
if __name__ == "__main__":
    check_device_online_offline()
