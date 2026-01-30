import os
import pymysql
import requests
import pytz
from datetime import datetime, timedelta
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException

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
        3: f"WARNING!! The {devnm} is offline. Please take necessary action- Regards Fertisense LLP",
        5: f"INFO!! The device {devnm} is back online. No action is required - Regards Fertisense LLP",
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
            unique_emails = list(set([e.strip() for e in emails if e.strip()]))
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

        # ================= ONLINE =================
        elif is_online and prev_is_active == 1:
        # elif is_online:
            sms_sent = False
            email_sent = False

            cursor.execute("""
                UPDATE device_status_alarm_log
                SET IS_ACTIVE = 0
                WHERE DEVICE_ID = %s AND IS_ACTIVE = 1
            """, (device_id,))

            # msg = f"INFO!! The {device_name} is back online. No action is required - Regards Fertisense LLP"
            msg = build_message(5, device_name)



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
            unique_emails = list(set([e.strip() for e in emails if e.strip()]))
            print("📧 Unique emails:", unique_emails)

            
            # 4️⃣ SEND SMS
            sms_sent = False
            for phone in unique_phones:
                if send_sms(msg, phone):
                    sms_sent = True

            # 5️⃣ SEND EMAIL
            email_sent = False
            for email in unique_emails:
                send_email("Device Online Info", msg, email)
                email_sent = True

            cursor.execute("""
                INSERT INTO device_status_alarm_log
                (DEVICE_ID, DEVICE_STATUS, IS_ACTIVE, CREATED_ON_DATE, CREATED_ON_TIME, SMS_DATE, SMS_TIME, EMAIL_DATE, EMAIL_TIME)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                device_id,
                0,
                0,
                now.date(),
                now.time(),
                now.date() if sms_sent else None,
                now.time() if sms_sent else None,
                now.date() if email_sent else None,
                now.time() if email_sent else None
            ))
            conn.commit()

    conn.close()

# =====================================================
# ENTRY POINT
# =====================================================
if __name__ == "__main__":
    check_device_online_offline()
