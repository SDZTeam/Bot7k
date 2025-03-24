import json

class Database:
    def __init__(self):
        self.data = {
            "chats": {},  # Группы чатов: {"Группа 1": [chat_id1, chat_id2], ...}
            "accounts": [],  # Список аккаунтов
            "messages": []  # Список сообщений
        }
        self.load_data()

    def load_data(self):
        try:
            with open("data.json", "r") as f:
                self.data = json.load(f)
        except FileNotFoundError:
            pass

    def save_data(self):
        with open("data.json", "w") as f:
            json.dump(self.data, f)

    def add_chat(self, group_name, chat_id):
        if group_name not in self.data["chats"]:
            self.data["chats"][group_name] = []
        if chat_id not in self.data["chats"][group_name]:
            self.data["chats"][group_name].append(chat_id)
            self.save_data()

    def remove_chat(self, group_name, chat_id):
        if group_name in self.data["chats"] and chat_id in self.data["chats"][group_name]:
            self.data["chats"][group_name].remove(chat_id)
            self.save_data()

    def add_account(self, account_token):
        self.data["accounts"].append({"token": account_token, "messages": []})
        self.save_data()

    def add_message(self, account_index, message):
        if 0 <= account_index < len(self.data["accounts"]):
            self.data["accounts"][account_index]["messages"].append(message)
            self.save_data()

    def get_chats(self):
        return self.data["chats"]

    def get_accounts(self):
        return self.data["accounts"]