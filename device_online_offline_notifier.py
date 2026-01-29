import os
import pymysql
import requests
from datetime import datetime, timedelta
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException

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
# BREVO EMAIL CONFIG
# =====================================================
BREVO_API_KEY = os.getenv("BREVO_API_KEY")
BREVO_SENDER_EMAIL = "fertisenseiot@gmail.com"   # VERIFIED
BREVO_SENDER_NAME = "Fertisense LLP"

# =====================================================
# SMS FUNCTION
# =====================================================
def send_sms(message, mobile):
    try:
        mobile = mobile.strip()
        if not mobile.startswith("91"):
            mobile = "91" + mobile
        
        payload = {
            "user": SMS_USER,
            "password": SMS_PASS,
            "senderid": SENDER_ID,
            "channel": "Trans",
            "DCS": 0,
            "flashsms": 0,
            "number": mobile,
            "text": message,
            "route": "1"
        }
        r= requests.get(SMS_API_URL, params=payload, timeout=10)
        
    except Exception as e:
        print("SMS Error:", e)

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

        api_instance.send_transac_email(email)

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

    now = datetime.now()
    five_min_ago = now - timedelta(minutes=5)

    # All devices
    cursor.execute("SELECT DISTINCT DEVICE_ID FROM device_reading_log")
    devices = cursor.fetchall()

    for d in devices:
        device_id = d["DEVICE_ID"]

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

        last_time = datetime.strptime(
            str(last["last_time"]), "%Y-%m-%d %H:%M:%S.%f"
        )

        is_online = last_time >= five_min_ago

        # Device org + centre
        cursor.execute("""
            SELECT ORGANIZATION_ID, CENTRE_ID
            FROM iot_api_masterdevice
            WHERE DEVICE_ID = %s
        """, (device_id,))
        device_info = cursor.fetchone()

        if not device_info:
            continue

        organization_id = device_info["ORGANIZATION_ID"]
        centre_id = device_info["CENTRE_ID"]

        # Previous status
        cursor.execute("""
            SELECT DEVICE_STATUS
            FROM device_status_alarm_log
            WHERE DEVICE_ID = %s
            ORDER BY DEVICE_STATUS_ALARM_ID DESC
            LIMIT 1
        """, (device_id,))
        prev = cursor.fetchone()
        prev_status = prev["DEVICE_STATUS"] if prev else None

        users = get_alert_users(cursor, organization_id, centre_id)

        # ================= OFFLINE =================
        if not is_online and prev_status != 1:
            msg = f"WARNING!! Device #{device_id} is OFFLINE. Please take necessary action."

            for user in users:
                if user["SEND_SMS"] == 1 and user["PHONE"]:
                    send_sms(msg, user["PHONE"])

                if user["SEND_EMAIL"] == 1 and user["EMAIL"]:
                    send_email("Device Offline Alert", msg, user["EMAIL"])

            cursor.execute("""
                INSERT INTO device_status_alarm_log
                (DEVICE_ID, DEVICE_STATUS, IS_ACTIVE, CREATED_ON_DATE, CREATED_ON_TIME)
                VALUES (%s, 1, 1, CURDATE(), CURTIME())
            """, (device_id,))
            conn.commit()

        # ================= ONLINE =================
        elif is_online and prev_status == 1:
            msg = f"INFO!! Device #{device_id} is back ONLINE. No action required."

            for user in users:
                if user["SEND_SMS"] == 1 and user["PHONE"]:
                    send_sms(msg, user["PHONE"])

                if user["SEND_EMAIL"] == 1 and user["EMAIL"]:
                    send_email("Device Online Info", msg, user["EMAIL"])

            cursor.execute("""
                INSERT INTO device_status_alarm_log
                (DEVICE_ID, DEVICE_STATUS, IS_ACTIVE, CREATED_ON_DATE, CREATED_ON_TIME)
                VALUES (%s, 0, 0, CURDATE(), CURTIME())
            """, (device_id,))
            conn.commit()

    conn.close()

# =====================================================
# ENTRY POINT
# =====================================================
if __name__ == "__main__":
    check_device_online_offline()
