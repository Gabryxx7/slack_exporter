"""
Author: Gabriele Marini @Gabryxx7

How to run it:
1. Create a Slack App (https://api.slack.com/start/overview#creating)
2. Give it all the permissions you need (usually all the read ones except for the admin persmissions)
3. Create a new config.yaml file that looks like this:

    SLACK_BOT_TOKEN: "xoxp-123abvcsafas-xxxxx-xxxxxx-..."
    timeframe:
      from: 2021-01-01
      to: 2021-02-02
    public_channels_file: public_channels.csv
    private_channels_file: private_channels.csv
    group_messages_file: group_messages.csv
    logger_name: slack_bot_log.txt

4. Get the conversation you want with:
    convos = get_conversations(types="public_channel,private_channel,im,mpim")
5. Export the data to csb with:
    export_all_conversations_history(convos)
"""

from datetime import datetime
import os
# Import WebClient from Python SDK (github.com/slackapi/python-slack-sdk)
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import yaml
import logging
# import utils
import csv
import os
import concurrent.futures
import time

class SlackCSVWriter():
    def __init__(self, filename, headers=None):
        self.filename = filename
        self.initialized = False
        self.file = open(self.filename, 'w', newline='', encoding='utf-8')
        self.csv_writer = csv.writer(self.file, delimiter=',', quoting=csv.QUOTE_ALL)
        self.headers = headers
        self.creation_time = datetime.now()
        if self.headers is not None:
            self.init(self.headers)
    
    def init(self, headers = None):
        # self.make_folder()
        # self.csv_writer.writerow(["convo_id", "convo_name", "convo_type", "msg_subtype", "msg_text", "msg_user_id", "msg_user_name", "msg_timestamp", "msg_datetime"])  
        if not self.initialized and headers is not None:
            self.csv_writer.writerow(headers)  
            self.initialized = True

    def close(self):
        self.file.close()


    def write_data(self, data_list, formatter=None, **kwargs):
        if not isinstance(data_list, list):
            data_list = [data_list]
        for data in data_list:
            if formatter is None:
                self.csv_writer.writerows(data)
            else:
                self.csv_writer.writerows(formatter(data, **kwargs))
    
    def dt_to_ts(datetime_str):
        # datetime_str = '2018-06-29 08:15:27.243860'
        datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        return datetime.timestamp(datetime_obj)

    def ts_to_dt(ts):
        return datetime.fromtimestamp(float(ts)).strftime('%Y-%m-%d %H:%M:%S')
        
    def format_message(message, users_list=None, prefix=None):
        prefix = prefix if prefix != None else []
        users_list = users_list if users_list != None else []
        rows = []
        try:
            row = []
            # row.append(message["message_id"])
            try:
                row.append(message["subtype"])
            except Exception as e:
                row.append("None")
            row.append(message["text"])
            try:
                row.append(message["user"])
            except Exception as e:
                row.append("None")
            try:
                row.append(users_list.get(user, "None"))
            except:
                row.append("None")
            row.append(message["ts"])
            row.append(SlackCSVWriter.ts_to_dt(message["ts"]))
            # print(f"{self.ts_to_dt(message['ts'])}\t{message['client_msg_id']}\t{message['type']}\t{message['user']}\t{message['team']}\t{message['text']}")
            rows.append(prefix+row)
        except Exception as e:
            print(f"error while writing to file: {e}")
        return rows

    def format_reaction(reaction, users_list=None, prefix=None):
        prefix = prefix if prefix != None else []
        users_list = users_list if users_list != None else []
        rows = []
        for user in reaction["users"]:
            try:
                row = []
                try:
                    row.append(reaction["name"])
                except Exception as e:
                    row.append("None")
                try:
                    row.append(user)
                except Exception as e:
                    row.append("None")            
                try:
                    row.append(users_list.get(user, "None"))
                except:
                    row.append("None")
                # print(f"{self.ts_to_dt(message['ts'])}\t{message['client_msg_id']}\t{message['type']}\t{message['user']}\t{message['team']}\t{message['text']}")
                rows.append(prefix+msg_prefix+row)
            except Exception as e:
                print(f"Error while writing to file: {e}")
        return rows

    def format_member(member, users_list=None, as_graph=False, prefix=None):      
        prefix = prefix if prefix != None else []
        users_list = users_list if users_list != None else []
        rows = []
        user_name = "None"
        try:
            user_name = users_list.get(user, "None")
        except Exception as e:
            print(f"Error getting {member} user_name: {e}")

        if as_graph:
            for p_member in members:
                p_user_name = "None"
                try:
                    p_user_name = users_list[p_member]
                except Exception as e:
                    print(f"Error getting second user {p_user_name} user_name: {e}")
                rows.append(prefix + [member, user_name, p_member, p_user_name])
        else:
            rows.append(prefix + [member,user_name])
        return rows

    def close(self):
        self.file.close()
        

class SlackExporter():
    class ExportType:
        Messages = "messages"
        Reactions = "reactions"
        Files = "files"
        Members = "members"

    def __init__(self, config_filename=None, bot_token=None):
        self.config = {}
        self.config_filename = config_filename

        if self.config_filename is not None:
            self.config = yaml.safe_load(open(self.config_filename))
        
        self.bot_token = self.config.get("SLACK_BOT_TOKEN", None)
        if self.bot_token is None:
            if bot_token is None:
                print("You need to provide a Slack BOT token!")
                return 
            self.bot_token = bot_token

        self.client = WebClient(token=self.bot_token)
            
        self.config['last_export_time'] = self.formatted_now()
        self.base_path = self.config.get("data_folder", "slack_export") +"\\" + self.config["last_export_time"]
        self.rate_limit = self.config.get("rate_limit", 80)
        self.rate_limit_wait = self.config.get("wait_time", 10)
        self.retry_delay = self.config.get("retry_delay", 5)
        self.logger_name = self.config.get("logger_name", "slack_log.log")
        self.logger = logging.getLogger(self.logger_name)
        self.calls_counter = 0
        self.started_time = datetime.now()
        self.processed_counter = 0
        self.users_list = None
        self.folder_created = False
        self.get_users_list()          

    def make_folder(self):
        if self.folder_created:
            return
        else:
            if not os.path.exists(self.base_path):
                os.makedirs(self.base_path)
            self.base_path += "\\"
        # Put users into the dict
    def users_to_dict(self, users_array):
        for user in users_array:
            # Key user info on their unique user ID
            user_id = user["id"]
            # Store the entire user object (you may not need all of the info)
            self.users_list[user_id] = user["name"]

    def read_users_list(self, filename, id_col=None, username_col=None):
        csv_users_file = filename
        self.users_list = {}
        if csv_users_file is None:
            csv_users_file = "slack_users_list.csv"
        else:
            try:
                with open(csv_users_file, mode='r', newline='') as infile:
                    reader = csv.reader(infile)
                    for rows in reader:
                        self.users_list[rows[0]] = rows[1]
            except IOError as e:
                print(f"Error reading users file: {e}")

    def update_users_list(self, filename=None):
        csv_users_file = filename
        if csv_users_file is None:
            csv_users_file = "slack_users_list.csv"
        print(f"Downloading user dictionary to {csv_users_file}")
        self.users_list = {}
        try:
            result = self.client.users_list()
            self.users_to_dict(result["members"])
            try:
                with open(csv_users_file, 'w', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile, delimiter=',')
                    csvwriter.writerow(["user_id", "user_name"])
                    for key, value in self.users_list.items():
                        csvwriter.writerow([key, value])
            except IOError as e:
                print(f"Error writing users file: {e}")
        except SlackApiError as e:
            self.logger.error("Error creating conversation: {}".format(e))
        return self.users_list
        
    def get_conversation_by_id(self, channel_id, conversations):
        res = next(filter(lambda x: 'id' in x and channel_id == x['id'], conversations), None)
        return res

    def get_conversation_by_name(self, channel_name, conversations):
        res = next(filter(lambda x: 'name' in x and channel_name == x['name'], conversations), None)
        return res

    def get_users_list(self):
        if self.users_list is None:
            self.update_users_list()
        return self.users_list
        
    def dt_to_ts(self, datetime_str):
        # datetime_str = '2018-06-29 08:15:27.243860'
        datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        return datetime.timestamp(datetime_obj)

    def ts_to_dt(self, ts):
        return datetime.fromtimestamp(float(ts)).strftime('%Y-%m-%d %H:%M:%S')

    def formatted_now(self, sepDate="_", sepTime="_", sep="_"):
        return datetime.now().strftime('%Y{0}%m{0}%d{2}%H{1}%M{1}%S'.format(sepDate, sepTime, sep))

    def retry_wait(self, sleep_time, message="Waiting"):
        for t in range(0, sleep_time):
            print(f"{message} {sleep_time-t} seconds...", end="\r", flush=True)
            time.sleep(1)
        print(f"{message} {sleep_time} seconds...")

    def check_rate_limit(self, started_time, rate_limit, wait_time):
        elapsed = (datetime.now() - started_time)
        elapsed_mins, elapsed_seconds= divmod(elapsed.seconds, 60)
        elapsed_mins += elapsed_seconds/60
        current_rate = 0 if elapsed_mins <= 0 else self.calls_counter/elapsed_mins
        # Just avoiding this to trigger at the start. Obviously if we did not even sent rate_limit calls we can't be over the limit
        if elapsed_mins > 0 and self.calls_counter > self.rate_limit:
            while current_rate > self.rate_limit:
                self.retry_wait(self.rate_limit_wait, f"Elapsed: {elapsed}, Calls: {self.calls_counter}, Rate: {current_rate:.2f}pm, Waiting")
                elapsed = (datetime.now() - started_time)
                elapsed_mins, elapsed_seconds= divmod(elapsed.seconds, 60)
                elapsed_mins += elapsed_seconds/60
                current_rate = 0 if elapsed_mins <= 0 else self.calls_counter/elapsed_mins
        return current_rate, elapsed
    
    def get_data_list(self, result, response_key=None, data_keys=None):
        response = result
        if response_key is not None:
            response = result[response_key]
        if data_keys is None:  
            return response
        
        data_list = []
        for data in response:
            res = {}
            for key in data_keys:
                res[key] = data[data_key]
            data_list.append(res)
        return data_list

    def get_data(self, client_method, client_args, response_key = None, data_keys = None, limit=200, processed=-1, total=-1):
        data_list = []
        method_name = client_method.__name__
        try:
            cursor = None
            while True:
                # Call the conversations.list method using the WebClient
                rate, elapsed = self.check_rate_limit(self.started_time, self.rate_limit, self.rate_limit_wait)
                try:
                    result = client_method(**client_args, limit=limit, cursor=cursor )
                    # result = self.client.conversations_list(types="public_channel,private_channel,mpim,im", limit=200, cursor=cursor)
                    self.calls_counter += 1
                except Exception as e:
                    print(f"Exception getting request {method_name}: {e}.\t Retrying in {self.retry_delay}")
                    self.retry_wait(self.retry_delay, f"Trying again in")
                    continue
                new_data = self.get_data_list(result, response_key, data_keys)  
                self.logger.info(new_data)
                # print(new_data[0])
                # print(new_data[0]["id"], new_data[-1]["id"])
                data_list += new_data
                if "response_metadata" in result:
                    cursor = result["response_metadata"].get("next_cursor", None)
                else:
                    cursor = None
                    # print(f"Exception getting conversations: {e}\t {len(convos)}")
                if processed > 0 and total > 0:
                    print(f"{processed:>4}/{total:<4}", end="")
                else:                    
                    print(f"{'-':>4}/{'-':<4}", end="")
                print(f"{client_method.__name__:15}\t Total data: {len(data_list):<4} \t Cursor: {str(cursor):<10}\tRate: {rate:>2.2f}\{self.rate_limit:<3} \tElapsed: {elapsed} \tArgs: {client_args}")
                if cursor is None or cursor == "":
                    break
            return data_list        
        except SlackApiError as e:
            print(f"Error in retreiving data from Slack API ({method_name}): {e}")

    def get_conversations(self, data_keys=None):
        client_args={"types":"public_channel,private_channel,mpim,im"}
        return self.get_data(self.client.conversations_list, client_args, response_key="channels", data_keys=None)

    """
    channel_id: ID of the channel you want to send the message to
    """
    def get_conversation_history(self, conversation, data_keys=None, processed=-1, total=-1, **kwargs):
        channel_id = conversation["id"]
        # print(f"ID2: {channel_id}")
        client_args={"channel":channel_id}
        # Call the conversations.history method using the WebClient
        # conversations.history returns the first 100 messages by default
        # These results are paginated, see: https://api.slack.com/methods/conversations.history$pagination
        messages = self.get_data(self.client.conversations_history, client_args, response_key="messages", data_keys=data_keys, processed=processed, total=total)
        self.logger.info(f"{len(messages)} messages found in {channel_id}")
        return messages

    
    def get_conversation_members(self, conversation, data_keys=None, get_user_info=True, processed=-1, total=-1, **kwargs):
        channel_id = conversation["id"]
        client_args={"channel":channel_id}
        members = self.get_data(self.client.conversations_members, client_args, response_key="members", data_keys=data_keys, processed=processed, total=total)
        self.logger.info(f"{len(members)} members in {channel_id}")
        if get_user_info:
            members = self.get_users_info(members)

        return members
    
    def get_message_reactions(self, channel_id, msg_timestamp, data_keys=None, processed=-1, total=-1, **kwargs):
        channel_id = conversation["id"]
        client_args={"channel":channel_id, "timestamp":msg_timestamp}
        reactions = self.get_data(self.client.reactions_get, client_args, response_key="message", data_keys=data_keys, processed=processed, total=total)
        self.logger.info(f"{len(reactions)} members in {channel_id}")
        return reactions
    
    def get_users_info(self, users_id_list, data_keys=None):
        users_data = []
        for idx, user_id in enumerate(users_id_list):
            client_args = {"user":user_id}
            users_data.append(self.get_data(self.client.users_info, client_args, response_key="user", data_keys=data_keys, processed=idx+1, total=len(users_id_list)))
        return users_data

    def print_conversations_list(self, convos):
        for conversation in convos:
            # print(conversation)
            try:
                name = self.get_conversation_name(conversation, users_list=self.users_list)
                type = self.get_conversation_type_string(conversation)
                print(f"{name}\t{type}")
            except KeyError as e:
                print(f"KeyError in retreiving key: {e}")
    
    def get_conversation_type_string(self, conversation):
        convo_type = ""
        if "is_channel" in conversation.keys() and conversation["is_channel"]:
            if conversation["is_private"]:
                convo_type = "private channel"
            else:
                convo_type = "public_channel"
        elif "is_group" in conversation.keys() and conversation["is_group"]:
            convo_type = "group"
        elif "is_im" in conversation.keys() and conversation["is_im"]:
            convo_type = "im"
        elif "is_mpim" in conversation.keys() and conversation["is_mpim"]:
            convo_type = "mpim"
        # elif conversation["is_group"]:
        #     convo_type = "group"
        return convo_type

    def get_conversation_name(self, conversation, users_list=None):
        convo_name = ""
        try:
            convo_name = conversation["name"]
        except KeyError as e:
            convo_name = conversation["user"]
            if users_list:
                convo_name = users_list[conversation["user"]]
        return convo_name


    def export_conversation_data(self, conversations, export_messages=True, export_messages_reactions=False, export_members=False, members_as_graph=False):
        self.make_folder()
        members_filename = f"{self.base_path}members_all_{self.config['last_export_time']}.csv"
        messages_filename = f"{self.base_path}messages_all_{self.config['last_export_time']}.csv"
        reactions_filename = f"{self.base_path}reactions_all_{self.config['last_export_time']}.csv"
        if not isinstance(conversations, list):
            convo_name = self.get_conversation_name(conversations, users_list=self.users_list)
            conversations = [conversations]
            members_filename = f"{self.base_path}{convo_name}_members_{self.config['last_export_time']}.csv"
            messages_filename = f"{self.base_path}{convo_name}_messages_{self.config['last_export_time']}.csv"
            reactions_filename = f"{self.base_path}{convo_name}_reactions_{self.config['last_export_time']}.csv"
    
        if export_members:
            headers = ["convo_id", "convo_name", "convo_type", "user_id","user_name"]
            if members_as_graph:
                headers += ["user_id2","user_name2"]
            members_csv = SlackCSVWriter(members_filename, headers)
        if export_messages:
            headers = ["convo_id", "convo_name", "convo_type", "msg_subtype", "msg_text", "msg_user_id", "msg_user_name", "msg_timestamp", "msg_datetime"]
            messages_csv = SlackCSVWriter(messages_filename, headers)
            if export_messages_reactions:
                headers = ["convo_id", "convo_name", "convo_type", "msg_timestamp", "msg_datetime", "reaction_name", "reaction_user", "reaction_username"]
                reactions_csv = SlackCSVWriter(reactions_filename, headers)

        for idx_c, conversation in enumerate(conversations):
            total=len(conversations)        
            processed=idx_c+1            
            conversation_id = conversation["id"]
            convo_info_prefix = [conversation_id, convo_name, self.get_conversation_type_string(conversation)]
            if export_members:
                members = self.get_conversation_members(conversation, processed=processed, total=total)
                members_csv.write_data(members, SlackCSVWriter.format_member, users_list=self.users_list, as_graph=members_as_graph, prefix=convo_info_prefix)
            if export_messages:
                messages = self.get_conversation_history(conversation, limit=200, processed=processed, total=total)
                for idx_m, message in enumerate(messages):
                    total=len(messages)
                    processed=idx_m+1
                    messages_csv.write_data(message, SlackCSVWriter.format_message, users_list=self.users_list, prefix=convo_info_prefix)
                    if export_messages_reactions:
                        msg_reactions = self.get_message_reactions(conversation_id, message["ts"], processed=processed, total=total)
                        if "reactions" in message:
                            msg_reactions = message["reactions"]
                            if msg_reactions is not None:
                                msg_prefix = []
                                msg_prefix.append(message["ts"])
                                msg_prefix.append(self.ts_to_dt(message["ts"]))
                                reactions_csv.write_data(msg_reactions, SlackCSVWriter.format_reaction, prefix=convo_info_prefix+msg_prefix)
            if export_members:
                print(f"------- MEMBERS EXPORT {os.path.basename(members_filename)} COMPLETED AT {self.formatted_now()} -------")
                members_csv.close()
            if export_messages:
                print(f"------- MESSAGES EXPORT {os.path.basename(messages_filename)} COMPLETED AT {self.formatted_now()} -------")
                messages_csv.close()
            if export_messages_reactions:
                print(f"------- REACTIONS EXPORT {os.path.basename(reactions_filename)} COMPLETED AT {self.formatted_now()} -------")
                reactions_csv.close()

        
    def export_all_conversations_history(self,conversations, export_reactions=True, multi_threaded=True, **kwargs):
        print("Exporting {} conversations".format(len(conversations)))
        f, csvwriter = init_csv_writer()
        if export_reactions:
            f_reactions, reactions_csvwriter = init_reactions_csv_writer()

        if multi_threaded:
            with concurrent.futures.ThreadPoolExecutor() as executor: # optimally defined number of threads
                res = []
                for i in range(0, len(conversations)):
                    res.append(executor.submit(self.export_conversation_history, conversations[i], csv_writer=csvwriter, export_reactions=True, reactions_csv_writer=reactions_csvwriter, messages=None, **kwargs))
                    if i > 0 and i % rate_limit == 0:
                        concurrent.futures.wait(res)
                        print(f"Waiting for the rate-limit of {i} every {sleep_time} seconds...")
                        for t in range(0, sleep_time):
                            print(f"Trying again in {sleep_time-t} seconds", end="\r", flush=True)
                            time.sleep(1)
        else:
            for conversation in conversations:
                self.export_conversation_history(conversation, csv_writer=csvwriter, export_reactions=True, reactions_csv_writer=reactions_csvwriter, messages=None, **kwargs)
        print(f"------- EXPORT {os.path.basename(f.name)} COMPLETED AT {self.formatted_now()} -------")
        print(f"------- REACTIONS EXPORT {os.path.basename(f_reactions.name)} COMPLETED AT {self.formatted_now()} -------")
        f.close()
        f_reactions.close()

    def export_all(self):
        print(f"Exporting All data:")

# def init():
    # WebClient instantiates a client that can call API methods
    # When using Bolt, you can use either `app.client` or the `client` passed to listeners.
    # The config data is stored in config.yaml

    # global config
    # config = yaml.safe_load(open("config.yaml"))
    # config['last_export_time'] = self.formatted_now()
    # global self.base_path
    # self.base_path = config["data_folder"] +"\\" + config["last_export_time"]
    # if not os.path.exists(self.base_path):
    #     os.makedirs(self.base_path)
    # self.base_path += "\\"
    # global client
    # client = WebClient(token=config["SLACK_BOT_TOKEN"])
    # global users_list
    # update_users_list(True)
    # global logger
    # logger = logging.getLogger(config["logger_name"])
    # global sleep_time
    # sleep_time = 30
    # global rate_limit
    # rate_limit = 80
    # global self.calls_counter
    # self.calls_counter = 0
    # global started_time
    # started_time = datetime.now()
    # global total_processed
    # total_processed = 0

def main():
    # init()
    convos = get_conversations(types="public_channel,private_channel,im,mpim")
    print_conversations_list(convos)
    # test = get_conversation_by_name("test", convos)
    # test_messages = get_conversation_history(test,
    #     inclusive=True,
    #     oldest=self.dt_to_ts("2018-01-01 01:00:00"),
    #     limit=100000000)
    # print(len(test_messages))
    # print(test_messages[0].keys())
    # export_conversation_history(test, messages=test_messages)
    # export_all_conversations_history(convos, multi_threaded=False,
    #     oldest=self.dt_to_ts("2018-01-01 01:00:00"),
    #     limit=100000000)

    export_all_conversations_members(convos, True)


if __name__ == "__main__":
    main()

