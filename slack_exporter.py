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

    def write_data(self, data_list, headers=None):
        self.init(headers)
        if not isinstance(data_list, list):
            data_list = [data_list]
        for data in data_list:
            self.csv_writer.writerow(data.values())

    def close():
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

    def make_folder(self):
        if self.folder_created:
            return
        else:
            if not os.path.exists(self.base_path):
                os.makedirs(self.base_path)
            self.base_path += "\\"
        # Put users into the dict
    def users_to_dict(users_array):
        for user in users_array:
            # Key user info on their unique user ID
            user_id = user["id"]
            # Store the entire user object (you may not need all of the info)
            users_list[user_id] = user["name"]

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
        users_list = {}
        try:
            result = self.client.users_list()
            users_to_dict(result["members"])
            try:
                with open(csv_users_file, 'w', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile, delimiter=',')
                    csvwriter.writerow(["user_id", "user_name"])
                    for key, value in users_list.items():
                        csvwriter.writerow([key, value])
            except IOError as e:
                print(f"Error writing users file: {e}")
        except SlackApiError as e:
            self.logger.error("Error creating conversation: {}".format(e))
        return users_list
        
    def get_conversation_by_id(self, channel_id, conversations):
        res = next(filter(lambda x: 'id' in x and channel_id == x['id'], conversations), None)
        return res

    def get_conversation_by_name(self, channel_name, conversations):
        res = next(filter(lambda x: 'name' in x and channel_name == x['name'], conversations), None)
        return res

    def get_users_list(self):
        if self.users_list is None:
            update_users_list()
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

    def get_data(self, client_method, client_args, response_key = None, data_keys = None, limit=200):
        data_list = []
        method_name = client_method.__name__
        try:
            while True:
                # Call the conversations.list method using the WebClient
                rate, elapsed = self.check_rate_limit(self.started_time, self.rate_limit, self.rate_limit_wait)
                cursor = None
                try:
                    result = client_method(**client_args, limit=limit, cursor=cursor )
                    # result = self.client.conversations_list(types="public_channel,private_channel,mpim,im", limit=200, cursor=cursor)
                    self.calls_counter += 1
                except Exception as e:
                    print(f"Exception getting request {method_name}: {e}.\t Retrying in {self.retry_delay}")
                    self.retry_wait(self.retry_delay, f"Trying again in")
                    continue
                data_list = self.get_data_list(result, response_key, data_keys)
                try:
                    cursor = result["response_metadata"]["next_cursor"]
                except Exception as e:
                    pass
                    # print(f"Exception getting conversations: {e}\t {len(convos)}")
                print(f"Total data: {len(data_list)} \t Cursor: {cursor}\tRate: {rate:.2f}\{self.rate_limit} \tElapsed: {elapsed}")
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
    def get_conversation_history(self, conversation, data_keys=None, **kwargs):
        channel_id = conversation["id"]
        print(f"ID2: {channel_id}")
        client_args={"channel":channel_id}
        # Call the conversations.history method using the WebClient
        # conversations.history returns the first 100 messages by default
        # These results are paginated, see: https://api.slack.com/methods/conversations.history$pagination
        messages = self.get_data(self.client.conversations_history, client_args, response_key="messages", data_keys=data_keys)
        self.logger.info(f"{len(messages)} messages found in {channel_id}")
        return messages

    
    def get_conversation_members(self, conversation, data_keys=None, get_user_info=True, **kwargs):
        channel_id = conversation["id"]
        client_args={"channel":channel_id}
        members = self.get_data(self.client.conversations_members, client_args, response_key="members", data_keys=data_keys)
        self.logger.info(f"{len(members)} members in {channel_id}")
        if get_user_info:
            members = self.get_users_info(members)

        return members
    
    def get_message_reactions(self, channel_id, msg_timestamp, data_keys=None, **kwargs):
        channel_id = conversation["id"]
        client_args={"channel":channel_id, "timestamp":msg_timestamp}
        reactions = self.get_data(self.client.reactions_get, client_args, response_key="message", data_keys=data_keys)
        self.logger.info(f"{len(reactions)} members in {channel_id}")
        return reactions
    
    def get_users_info(self, users_id_list, data_keys=None):
        users_data = []
        for user_id in users_id_list:
            client_args = {"user":user_id}
            users_data.append(self.get_data(self.client.users_info, client_args, response_key="user", data_keys=data_keys))
        return users_data

    def print_conversations_list(convos):
        for conversation in convos:
            # print(conversation)
            try:
                name = self.get_conversation_name(conversation, users_list=users_list)
                type = self.get_conversation_type_string(conversation)
                # print(f"{name}\t{type}")
            except KeyError as e:
                print(f"KeyError in retreiving key: {e}")

    def write_messages(self,messages, csvwriter, reactions_csv_writer=None, prefix=None):
        prefix = prefix if prefix != None else []
        elapsed = (datetime.now() - started_time)
        elapsed_mins = elapsed.seconds // 60 % 60
        calls_per_min = "-"
        if elapsed_mins > 0:
            calls_per_min = self.calls_counter/elapsed_mins
            wait_time = 10
            while calls_per_min > rate_limit:
                print(f"Elapsed: {elapsed}, Calls: {self.calls_counter}, Rate: {calls_per_min:.2f}pm, Waiting {wait_time} seconds...")
                for t in range(0,wait_time):
                    time.sleep(1)
                    print(f"Elapsed: {elapsed}, Calls: {self.calls_counter}, Rate: {calls_per_min:.2f}pm, Waiting {wait_time-t} seconds...", end="\r", flush=True)
                elapsed_mins = (datetime.now() - started_time).seconds // 60 % 60
                calls_per_min = self.calls_counter/elapsed_mins
            
        print(f"Elapsed: {elapsed}, Calls: {self.calls_counter}, Rate: {calls_per_min:.2f}pm, Exporting {len(messages)} messages for: {prefix}")
        for message in messages:
            write_message(message, csvwriter, prefix)
            if reactions_csv_writer is not None:
                export_message_reactions(prefix[0], message, reactions_csv_writer, prefix)


    def write_message(self,message, csvwriter, prefix=None):
        prefix = prefix if prefix != None else []
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
                row.append(users_list[message["user"]])
            except:
                row.append("None")
            row.append(message["ts"])
            row.append(self.ts_to_dt(message["ts"]))
            # print(f"{self.ts_to_dt(message['ts'])}\t{message['client_msg_id']}\t{message['type']}\t{message['user']}\t{message['team']}\t{message['text']}")
            csvwriter.writerow(prefix+row)
        except Exception as e:
            print(f"error while writing to file: {e}")

    def write_reactions(self,reactions, message, csvwriter, prefix=None):
        prefix = prefix if prefix != None else []
        msg_prefix = []
        msg_prefix.append(message["ts"])
        msg_prefix.append(self.ts_to_dt(message["ts"]))
        # print("Exporting {} reactions for: {}".format(len(reactions), msg_prefix))
        for reaction in reactions:
            write_reaction(reaction, csvwriter, prefix+msg_prefix)
            
    def write_reaction(self,reaction, csvwriter, prefix=None):
        prefix = prefix if prefix != None else []
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
                    row.append(users_list[user])
                except:
                    row.append("None")
                # print(f"{self.ts_to_dt(message['ts'])}\t{message['client_msg_id']}\t{message['type']}\t{message['user']}\t{message['team']}\t{message['text']}")
                csvwriter.writerow(prefix+row)
            except Exception as e:
                print(f"Error while writing to file: {e}")

    def write_members(self,members, as_graph, csvwriter, prefix=None):
        prefix = prefix if prefix != None else []
        print("Exporting {} members for: {}".format(len(members), prefix))
        for member in members:
            write_member(member, as_graph, members, csvwriter, prefix)

    def write_member(self,member, as_graph, members, csvwriter, prefix=None):
        user_name = "None"
        try:
            user_name = users_list[member]
        except Exception as e:
            print(f"Error getting {member} user_name: {e}")

        if as_graph:
            for p_member in members:
                p_user_name = "None"
                try:
                    p_user_name = users_list[p_member]
                except Exception as e:
                    print(f"Error getting second user {p_user_name} user_name: {e}")
                csvwriter.writerow(prefix + [member, user_name, p_member, p_user_name])
        else:
            csvwriter.writerow(prefix + [member,user_name])

    def export_message_reactions(self,conversation_id, message, reactions_csv_writer, convo_info_prefix, **kwargs):
        # msg_reactions = get_message_reactions(conversation_id, message["ts"])
        if "reactions" in message:
            msg_reactions = message["reactions"]
            if msg_reactions is not None:
                write_reactions(msg_reactions, message, csvwriter=reactions_csv_writer, prefix=convo_info_prefix)

    def export_conversation_members(self,conversation, as_graph=True, csv_writer=None):
        convo_name = self.get_conversation_name(conversation, users_list=users_list)
        members = get_conversation_members(conversation)
        csvwriter = csv_writer
        if csvwriter is None:
            f_members, csvwriter = init_members_csv_writer(convo_name)
        convo_info_prefix = [conversation["id"], convo_name, self.get_conversation_type_string(conversation)]
        write_members(members, as_graph, csvwriter=csvwriter, prefix=convo_info_prefix)
        if csv_writer is None:  # if we created a new writer inside here then we need to close the file at the end
            print(f"------- EXPORT {os.path.basename(f_members.name)} COMPLETED AT {self.formatted_now()} -------")
            f_members.close()

    def export_conversation_history(self, conversations):
        if not isinstance(conversations, list):
            convo_name = self.get_conversation_name(conversation, users_list=users_list)
            conversations = [conversations]
            messages_csv = SlackCSVWriter(convo_name)
            reactions_csv = SlackCSVWriter(convo_name)
        else:

        for conversation in conversations:
            messages = get_conversation_history(conversation, limit=200)


    def export_conversation_history(self,conversation, csv_writer=None, export_reactions=True, reactions_csv_writer=None, messages=None, **kwargs):
        convo_name = self.get_conversation_name(conversation, users_list=users_list)
        if messages == None:
            messages = get_conversation_history(conversation, limit=200)
        csvwriter = csv_writer
        reactions_csvwriter = reactions_csv_writer
        if csvwriter is None:
            f, csvwriter = init_csv_writer(convo_name)
            if export_reactions is None:
                f_reactions, reactions_csvwriter = init_reactions_csv_writer(convo_name)

        convo_info_prefix = [conversation["id"], convo_name, self.get_conversation_type_string(conversation)]
        write_messages(messages, csvwriter=csvwriter, reactions_csv_writer=reactions_csvwriter, prefix=convo_info_prefix)
        if csv_writer is None:  # if we created a new writer inside here then we need to close the file at the end
            print(f"------- EXPORT {os.path.basename(f.name)} COMPLETED AT {self.formatted_now()} -------")
            f.close()
            if export_reactions:
                print(f"------- REACTIONS EXPORT {os.path.basename(f_reactions.name)} COMPLETED AT {self.formatted_now()} -------")
            f_reactions.close()
        
    def export_all_conversations_history(self,conversations, export_reactions=True, multi_threaded=True, **kwargs):
        print("Exporting {} conversations".format(len(conversations)))
        f, csvwriter = init_csv_writer()
        if export_reactions:
            f_reactions, reactions_csvwriter = init_reactions_csv_writer()

        if multi_threaded:
            with concurrent.futures.ThreadPoolExecutor() as executor: # optimally defined number of threads
                res = []
                for i in range(0, len(conversations)):
                    res.append(executor.submit(export_conversation_history, conversations[i], csv_writer=csvwriter, export_reactions=True, reactions_csv_writer=reactions_csvwriter, messages=None, **kwargs))
                    if i > 0 and i % rate_limit == 0:
                        concurrent.futures.wait(res)
                        print(f"Waiting for the rate-limit of {i} every {sleep_time} seconds...")
                        for t in range(0, sleep_time):
                            print(f"Trying again in {sleep_time-t} seconds", end="\r", flush=True)
                            time.sleep(1)
        else:
            for conversation in conversations:
                export_conversation_history(conversation, csv_writer=csvwriter, export_reactions=True, reactions_csv_writer=reactions_csvwriter, messages=None, **kwargs)
        print(f"------- EXPORT {os.path.basename(f.name)} COMPLETED AT {self.formatted_now()} -------")
        print(f"------- REACTIONS EXPORT {os.path.basename(f_reactions.name)} COMPLETED AT {self.formatted_now()} -------")
        f.close()
        f_reactions.close()

    def export_all_conversations_members(self, conversations, as_graph=True, **kwargs):
        print("Exporting {} conversation members".format(len(conversations)))  
        f_members, csvwriter = init_members_csv_writer()
        for conversation in conversations:
            export_conversation_members(conversation, as_graph, csv_writer=csvwriter, **kwargs)
        print(f"------- EXPORT {os.path.basename(f_members.name)} COMPLETED AT {self.formatted_now()} -------")
        f_members.close()


    def init_members_csv_writer(self,convo_name=None, as_graph=True):
        self.make_folder()
        filename = f"{self.base_path}slack_members_export_{config['last_export_time']}.csv"
        if convo_name is not None:
            filename = f"{self.base_path}slack_{convo_name}_members_export_{config['last_export_time']}.csv"

        f = open(filename, 'w', newline='', encoding='utf-8')
        print(f"Initialized file {filename}")
        csvwriter = csv.writer(f, delimiter=',', quoting=csv.QUOTE_ALL)
        headers = ["convo_id", "convo_name", "convo_type", "user_id","user_name"]
        if as_graph:
            headers += ["user_id2","user_name2"]
        csvwriter.writerow(headers)     # header
        return f, csvwriter

    def init_csv_writer(self,convo_name=None):
        self.make_folder()
        filename = f"{self.base_path}slack_export_{config['last_export_time']}.csv"
        if convo_name is not None:
            filename = f"{self.base_path}slack_{convo_name}_export_{config['last_export_time']}.csv"

        f = open(filename, 'w', newline='', encoding='utf-8')
        csvwriter = csv.writer(f, delimiter=',', quoting=csv.QUOTE_ALL)
        csvwriter.writerow(["convo_id", "convo_name", "convo_type", "msg_subtype", "msg_text", "msg_user_id", "msg_user_name", "msg_timestamp", "msg_datetime"])     # header
        return f, csvwriter

    def init_reactions_csv_writer(self,convo_name=None):
        reactions_filename = f"{self.base_path}slack_reactions_export_{config['last_export_time']}.csv"
        if convo_name is not None:
            reactions_filename = f"{self.base_path}slack_{convo_name}_reactions_export_{config['last_export_time']}.csv"
        f = open(reactions_filename, 'w', newline='', encoding='utf-8')
        csvwriter = csv.writer(f, delimiter=',', quoting=csv.QUOTE_ALL)
        csvwriter.writerow(["convo_id", "convo_name", "convo_type", "msg_timestamp", "msg_datetime", "reaction_name", "reaction_user", "reaction_username"])     # header
        return f, csvwriter

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

