import imaplib
import logging
import pathlib
import queue
import threading
from typing import Dict, Union, Optional
from bs4 import BeautifulSoup
from multiprocessing.pool import ThreadPool
import getpass
import email
import json
import re
import time
import requests
import pickledb

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
NUM_THREADS = 4
email_dir = pathlib.Path("./email_files")
pdb = pickledb.load("data.db", True)
my_email_addr = input("Enter your gmail addr: ").strip()

import os
get_pass_fn = input
if os.name != "nt":
    get_pass_fn = getpass.getpass

imap_password = get_pass_fn("Enter your gmail application (IMAP) password: ")


mail_search_string = "ALL"
search_only_unread = input("Enter 'y' to run only over unread emails (any other answer means running on everything): ")
if search_only_unread.lower().strip() == "y":
    mail_search_string = "(UNSEEN)"


def connect_to_gmail_imap() -> imaplib.IMAP4_SSL:
    imap_url = 'imap.gmail.com'
    mail = imaplib.IMAP4_SSL(imap_url)
    mail.login(my_email_addr, imap_password)
    mail.select(mailbox='INBOX')
    return mail


class WriteThread(threading.Thread):

    def __init__(self):
        super().__init__()
        self.queue = queue.Queue()

    def run(self):
        while True:
            if self.queue.empty():
                time.sleep(0.1)
            idx, item = self.queue.get()
            pdb.set(idx, item)


class TrasherThread(threading.Thread):

    def __init__(self):
        super().__init__()
        self.mail_client = connect_to_gmail_imap()
        self.queue = queue.Queue()

    def move_gmail_to_trash(self, uid: str):
        self.mail_client.uid('STORE', uid, '+X-GM-LABELS', '\\Trash')

    def run(self):
        while True:
            if self.queue.empty():
                time.sleep(0.1)
            uid = self.queue.get()
            self.move_gmail_to_trash(uid)
            logger.info(f"Deleting uid {uid}")
            time.sleep(0.35) #  hope we don't get rate limited...


pdb_writer = WriteThread()
pdb_writer.start()
trasher = TrasherThread()
trasher.start()


def extract_data(msg: email.message.Message) -> Dict[str, Union[str, bytes, int, float]]:
    data = {}
    data["body"] = ""
    for part in msg.walk():
        data.update(part.items())
        if "text" in part.get_content_type():
            payload = str(part.get_payload(decode=True))
            soup = BeautifulSoup(payload)
            txt = re.sub(' +', ' ', soup.get_text())
            data["body"] += txt
    return data


def get_result(message: Dict[str, Union[str, bytes, int, float]]) -> Dict[str, Union[str, bytes, int, float]]:
    payload = {
        "model": "llama3.1",
        "prompt": f"""
         Please mark it for deletion if it is:
             * [CODE 0 - DELETE] a promotion of any kind
             * [CODE 1 - DELETE] from any social media site such as facebook, instagram, linkedin, etc.
             * [CODE 2 - DELETE] advertising of any kind
             * [CODE 3 - DELETE] newsletters, or digests, or article dumps
             * [CODE 4 - DELETE] marketing 
             * [CODE 5 - DELETE] automated messages, unless sharing a document 
         I'm interested in keeping: 
             * [CODE 6 - KEEP] Please keep legitimate invoices
             * [CODE 7 - KEEP] Please keep anything from a real human, even if I am not the direct recipient.
             * [CODE 8 - KEEP] Please keep everything I sent to myself {my_email_addr}.
             * [CODE 9 - KEEP] Please keep anything regarding taxes, accounting, banking, or financial information.
             * [CODE 10 - KEEP] Please keep bills
             * [CODE 11 - KEEP] Please keep order receipts
             * [CODE 12 - KEEP] Please keep conversations between myself and another individual on a third party platform such as craigslist.
             * [CODE 13 - KEEP] Please keep anything with an attachment.
             * [CODE 14 - KEEP] Please keep group emails
             * [CODE 15 - KEEP] Please ALWAYS keep personal messages between me (mike.pfaffenberger@gmail.com) and others.
             * [CODE 16 - KEEP] Please keep any registration emails that contain usernames, since I always lose those :(
             * [CODE 17 - KEEP] Please keep any emails related to job searching or recruitment   
        
            Here are the relevant fields of the email message:
                *** BEGIN MESSAGE ***
                From: {message["From"]}.
                To:  {message["To"]}.
                Date: {message["Date"]}
                Body: {message["body"]}
                *** END MESSAGE ***
            
            Reply only with 'KEEP', 'DELETE', followed by a newline symbol, and then a very short sentence explaining your reasoning and citing the [CODE x].
             It is fine if there are multiple reasons, you can list multiple.
        """,
        "stream": False,
        "options": {
            "temperature": 0
        },
    }
    result = requests.post("http://localhost:11434/api/generate", json=payload)
    result = result.json()['response']
    return result


def process_and_delete_email_idx(idx: str) -> Optional[Dict[str, Union[str, bytes, int, float]]]:
    result = process_email_idx(idx)
    if result["decision"] == "DELETE":
        trasher.queue.put(idx)
    return result

def process_email_idx(idx: str) -> Optional[Dict[str, Union[str, bytes, int, float]]]:
    try:
        maybe_result = pdb.get(idx)
        if maybe_result:
            logger.info(f"Cache hit on idx: {idx}")
            return maybe_result
        mail_client_ = connect_to_gmail_imap()
        status, msg = mail_client_.fetch(str(idx), '(RFC822)')
        email_fields = extract_data(email.message_from_bytes(msg[0][1]))
        keep = get_result(email_fields)
        decision = keep.split("\n")[0]
        reason = keep.split("\n")[1]
        email_fields["decision"] = decision.replace("\r", "")
        email_fields["reason" ] = reason
        logger.info(json.dumps(email_fields, indent=2))
        logger.info(f"idx: {idx}, Decision: {decision} Reason: {reason}")
        id_key = "Message-ID"
        if id_key not in email_fields:
            id_key = "Message-Id"
        fn = re.sub('[^0-9a-zA-Z]+', '_', email_fields[id_key].replace(" ", "_").lower().strip())
        file_path = email_dir / (fn + ".eml")
        f = open(file_path, "wb")
        f.write(msg[0][1])
        f.close()
        pdb_writer.queue.put((idx, email_fields))
        mail_client_.close()
    except Exception as e:
        logger.error("Failed to LLM it", exc_info=e)
        return None
    return email_fields


if __name__ == "__main__":
    mail_client = connect_to_gmail_imap()

    status, ids = mail_client.search(None, "ALL")
    ids = ids[0].decode().split(" ")

    tp = ThreadPool(NUM_THREADS)
    tp.map(process_and_delete_email_idx, ids)

    keys = pdb.getall()
    records = [(key, pdb.get(key)) for key in keys]
    deleted_records = [(key, record) for key, record in records if record['decision'] == 'DELETE']
    num_deletions = len(deleted_records)
    logger.info(f"Marked {num_deletions} as deleted / trashed.")
    logger.info("Finished ...")
    trasher.join()
