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
import utils
import csv
import os
import concurrent.futures
import time


def get_conversations(**kwargs):
    global total_calls
    convos = []
    cursor = None
    try:
        while True:
            # Call the conversations.list method using the WebClient
            cursor = None
            try:
                result = client.conversations_list(types="public_channel,private_channel,mpim,im", limit=200, cursor=cursor)
                total_calls += 1
            except Exception as e:
                print(f"Exception sending conversations_list request {e}, retrying")
                continue
            for response in result:
                convos +=response['channels']
            print(f"Total conversations: {len(convos)}\t{cursor}")
            try:
                cursor = result["response_metadata"]["next_cursor"]
            except Exception as e:
                pass
                # print(f"Exception getting conversations: {e}\t {len(convos)}")

            if cursor is None or cursor == "":
                break
        return convos        
    except SlackApiError as e:
        print(f"Error in retreiving conversations: {e}")

"""
 channel_id: ID of the channel you want to send the message to
"""
def get_conversation_history(conversation, **kwargs):
    global total_calls
    channel_id = conversation["id"]
    conversation_history = []
    cursor = None
    try:
        while True:
            # Call the conversations.history method using the WebClient
            # conversations.history returns the first 100 messages by default
            # These results are paginated, see: https://api.slack.com/methods/conversations.history$pagination
            try:
                result = client.conversations_history(channel=channel_id, cursor=cursor, **kwargs)
                total_calls += 1
            except Exception as e:
                print(f"Exception sending conversations_history request for {conversation['id']}: {e},\n waiting for {sleep_time}s and retrying")
                continue
            conversation_history += result["messages"]
            cursor = None
            try:
                cursor = result["response_metadata"]["next_cursor"]
            except Exception as e:
                pass
                # print(f"Total messages: {len(conversation_history)}")
                # Print results
            
            if cursor is None or cursor == "":
                break
        logger.info(f"{len(conversation_history)} messages found in {id}")
        return conversation_history
    except SlackApiError as e:
        logger.error(f"Error getting conversation. Error: {result['error']} \nException: {e}")

def get_conversation_members(conversation, **kwargs):
    global total_calls
    channel_id = conversation["id"]
    conversation_members = []
    cursor = None
    try:
        while True:
            # Call the conversations.history method using the WebClient
            # conversations.history returns the first 100 messages by default
            # These results are paginated, see: https://api.slack.com/methods/conversations.history$pagination
            try:
                result = client.conversations_members(channel=channel_id, cursor=cursor, **kwargs)
                total_calls += 1
            except Exception as e:
                print(f"Exception sending conversations_members request for {conversation['id']}: {e},\n waiting for {sleep_time}s and retrying")
                continue
            conversation_members += result["members"]
            cursor = None
            try:
                cursor = result["response_metadata"]["next_cursor"]
            except Exception as e:
                pass
            
            if cursor is None or cursor == "":
                break
        logger.info(f"{len(conversation_members)} members found in {id}")
        return conversation_members
    except SlackApiError as e:
        logger.error(f"Error getting conversation. Error: {result['error']} \nException: {e}")

def get_message_reactions(channel_id, msg_timetsamp, **kwargs):
    global total_calls
    msg_reactions = []
    try:
        # print("Requesting msg {} from channel {}".format(msg_timetsamp, channel_id))
        result = client.reactions_get(channel=channel_id, timestamp=msg_timetsamp, **kwargs)
        total_calls += 1
        # utils.print_dict_keys(result.data)
        msg_reactions = result["message"]
        # utils.print_dict_keys(msg_reactions)
        try:
            # Print results
            # logger.info("{} reactions found in {}".format(len(msg_reactions["reactions"]), id))
            return msg_reactions
        except Exception as e:
            print("Error getting reactions for conversation: {}\n Result Keys {}".format(e, utils.print_dict_keys(result.data)))
            return None
    except SlackApiError as e:
        logger.error("Error getting reactions for conversation: {}".format(e))

def get_conversation_by_id(channel_id, conversations):
    res = next(filter(lambda x: 'id' in x and channel_id == x['id'], conversations), None)
    return res

def get_conversation_by_name(channel_name, conversations):
    res = next(filter(lambda x: 'name' in x and channel_name == x['name'], conversations), None)
    return res

def get_user_info(user_id):
    global total_calls
    try:
        result = client.users_info(user=users_list)
        total_calls += 1
        # print(result)
    except SlackApiError as e:
        logger.error("Error creating conversation: {}".format(e))

# Put users into the dict
def users_to_dict(users_array):
    for user in users_array:
        # Key user info on their unique user ID
        user_id = user["id"]
        # Store the entire user object (you may not need all of the info)
        users_list[user_id] = user["name"]

def update_users_list(force_overwrite=False):
    global users_list
    users_list = {}
    csv_users_file = "slack_users_list.csv"
    try:
        with open(csv_users_file, mode='r', newline='') as infile:
            reader = csv.reader(infile)
            for rows in reader:
                users_list[rows[0]] = rows[1]
    except IOError as e:
        print(f"Error reading users file: {e}")
    if not users_list or force_overwrite:
        print("Downloading user dictionary")
        users_list = {}
        try:
            result = client.users_list()
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
            logger.error("Error creating conversation: {}".format(e))
    return users_list

def print_conversations_list(convos):
    for conversation in convos:
        # print(conversation)
        try:
            name = utils.get_conversation_name(conversation, users_list=users_list)
            type = utils.get_conversation_type_string(conversation)
            # print(f"{name}\t{type}")
        except KeyError as e:
            print(f"KeyError in retreiving key: {e}")

def write_messages(messages, csvwriter, reactions_csv_writer=None, prefix=None):
    prefix = prefix if prefix != None else []
    elapsed = (datetime.now() - started_time)
    elapsed_mins = elapsed.seconds // 60 % 60
    calls_per_min = "-"
    if elapsed_mins > 0:
        calls_per_min = total_calls/elapsed_mins
        wait_time = 10
        while calls_per_min > rate_limit:
            print(f"Elapsed: {elapsed}, Calls: {total_calls}, Rate: {calls_per_min:.2f}pm, Waiting {wait_time} seconds...")
            for t in range(0,wait_time):
                time.sleep(1)
                print(f"Elapsed: {elapsed}, Calls: {total_calls}, Rate: {calls_per_min:.2f}pm, Waiting {wait_time-t} seconds...", end="\r", flush=True)
            elapsed_mins = (datetime.now() - started_time).seconds // 60 % 60
            calls_per_min = total_calls/elapsed_mins
        
    print(f"Elapsed: {elapsed}, Calls: {total_calls}, Rate: {calls_per_min:.2f}pm, Exporting {len(messages)} messages for: {prefix}")
    for message in messages:
        write_message(message, csvwriter, prefix)
        if reactions_csv_writer is not None:
            export_message_reactions(prefix[0], message, reactions_csv_writer, prefix)


def write_message(message, csvwriter, prefix=None):
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
        row.append(utils.ts_to_dt(message["ts"]))
        # print(f"{utils.ts_to_dt(message['ts'])}\t{message['client_msg_id']}\t{message['type']}\t{message['user']}\t{message['team']}\t{message['text']}")
        csvwriter.writerow(prefix+row)
    except Exception as e:
        print(f"error while writing to file: {e}")

def write_reactions(reactions, message, csvwriter, prefix=None):
    prefix = prefix if prefix != None else []
    msg_prefix = []
    msg_prefix.append(message["ts"])
    msg_prefix.append(utils.ts_to_dt(message["ts"]))
    # print("Exporting {} reactions for: {}".format(len(reactions), msg_prefix))
    for reaction in reactions:
        write_reaction(reaction, csvwriter, prefix+msg_prefix)
        
def write_reaction(reaction, csvwriter, prefix=None):
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
            # print(f"{utils.ts_to_dt(message['ts'])}\t{message['client_msg_id']}\t{message['type']}\t{message['user']}\t{message['team']}\t{message['text']}")
            csvwriter.writerow(prefix+row)
        except Exception as e:
            print(f"Error while writing to file: {e}")

def write_members(members, as_graph, csvwriter, prefix=None):
    prefix = prefix if prefix != None else []
    print("Exporting {} members for: {}".format(len(members), prefix))
    for member in members:
        write_member(member, as_graph, members, csvwriter, prefix)

def write_member(member, as_graph, members, csvwriter, prefix=None):
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

def export_message_reactions(conversation_id, message, reactions_csv_writer, convo_info_prefix, **kwargs):
    # msg_reactions = get_message_reactions(conversation_id, message["ts"])
    if "reactions" in message:
        msg_reactions = message["reactions"]
        if msg_reactions is not None:
            write_reactions(msg_reactions, message, csvwriter=reactions_csv_writer, prefix=convo_info_prefix)

def export_conversation_members(conversation, as_graph=True, csv_writer=None):
    convo_name = utils.get_conversation_name(conversation, users_list=users_list)
    members = get_conversation_members(conversation)
    csvwriter = csv_writer
    if csvwriter is None:
        f_members, csvwriter = init_members_csv_writer(convo_name)
    convo_info_prefix = [conversation["id"], convo_name, utils.get_conversation_type_string(conversation)]
    write_members(members, as_graph, csvwriter=csvwriter, prefix=convo_info_prefix)
    if csv_writer is None:  # if we created a new writer inside here then we need to close the file at the end
        print(f"------- EXPORT {os.path.basename(f_members.name)} COMPLETED AT {utils.formatted_now()} -------")
        f_members.close()

def export_conversation_history(conversation, csv_writer=None, export_reactions=True, reactions_csv_writer=None, messages=None, **kwargs):
    convo_name = utils.get_conversation_name(conversation, users_list=users_list)
    if messages == None:
        messages = get_conversation_history(conversation, limit=200)
    csvwriter = csv_writer
    reactions_csvwriter = reactions_csv_writer
    if csvwriter is None:
        f, csvwriter = init_csv_writer(convo_name)
        if export_reactions is None:
            f_reactions, reactions_csvwriter = init_reactions_csv_writer(convo_name)

    convo_info_prefix = [conversation["id"], convo_name, utils.get_conversation_type_string(conversation)]
    write_messages(messages, csvwriter=csvwriter, reactions_csv_writer=reactions_csvwriter, prefix=convo_info_prefix)
    if csv_writer is None:  # if we created a new writer inside here then we need to close the file at the end
        print(f"------- EXPORT {os.path.basename(f.name)} COMPLETED AT {utils.formatted_now()} -------")
        f.close()
        if export_reactions:
            print(f"------- REACTIONS EXPORT {os.path.basename(f_reactions.name)} COMPLETED AT {utils.formatted_now()} -------")
        f_reactions.close()
    
def export_all_conversations_history(conversations, export_reactions=True, multi_threaded=True, **kwargs):
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
    print(f"------- EXPORT {os.path.basename(f.name)} COMPLETED AT {utils.formatted_now()} -------")
    print(f"------- REACTIONS EXPORT {os.path.basename(f_reactions.name)} COMPLETED AT {utils.formatted_now()} -------")
    f.close()
    f_reactions.close()

def export_all_conversations_members(conversations, as_graph=True, **kwargs):
    print("Exporting {} conversation members".format(len(conversations)))  
    f_members, csvwriter = init_members_csv_writer()
    for conversation in conversations:
        export_conversation_members(conversation, as_graph, csv_writer=csvwriter, **kwargs)
    print(f"------- EXPORT {os.path.basename(f_members.name)} COMPLETED AT {utils.formatted_now()} -------")
    f_members.close()


def init_members_csv_writer(convo_name=None, as_graph=True):
    filename = f"{base_folder_path}slack_members_export_{config['last_export_time']}.csv"
    if convo_name is not None:
        filename = f"{base_folder_path}slack_{convo_name}_members_export_{config['last_export_time']}.csv"

    f = open(filename, 'w', newline='', encoding='utf-8')
    print(f"Initialized file {filename}")
    csvwriter = csv.writer(f, delimiter=',', quoting=csv.QUOTE_ALL)
    headers = ["convo_id", "convo_name", "convo_type", "user_id","user_name"]
    if as_graph:
        headers += ["user_id2","user_name2"]
    csvwriter.writerow(headers)     # header
    return f, csvwriter

def init_csv_writer(convo_name=None):
    filename = f"{base_folder_path}slack_export_{config['last_export_time']}.csv"
    if convo_name is not None:
        filename = f"{base_folder_path}slack_{convo_name}_export_{config['last_export_time']}.csv"

    f = open(filename, 'w', newline='', encoding='utf-8')
    csvwriter = csv.writer(f, delimiter=',', quoting=csv.QUOTE_ALL)
    csvwriter.writerow(["convo_id", "convo_name", "convo_type", "msg_subtype", "msg_text", "msg_user_id", "msg_user_name", "msg_timestamp", "msg_datetime"])     # header
    return f, csvwriter

def init_reactions_csv_writer(convo_name=None):
    reactions_filename = f"{base_folder_path}slack_reactions_export_{config['last_export_time']}.csv"
    if convo_name is not None:
        reactions_filename = f"{base_folder_path}slack_{convo_name}_reactions_export_{config['last_export_time']}.csv"
    f = open(reactions_filename, 'w', newline='', encoding='utf-8')
    csvwriter = csv.writer(f, delimiter=',', quoting=csv.QUOTE_ALL)
    csvwriter.writerow(["convo_id", "convo_name", "convo_type", "msg_timestamp", "msg_datetime", "reaction_name", "reaction_user", "reaction_username"])     # header
    return f, csvwriter

def init():
    # WebClient instantiates a client that can call API methods
    # When using Bolt, you can use either `app.client` or the `client` passed to listeners.
    # The config data is stored in config.yaml

    global config
    config = yaml.safe_load(open("config.yaml"))
    config['last_export_time'] = utils.formatted_now()
    global base_folder_path
    base_folder_path = config["data_folder"] +"\\" + config["last_export_time"]
    if not os.path.exists(base_folder_path):
        os.makedirs(base_folder_path)
    base_folder_path += "\\"
    global client
    client = WebClient(token=config["SLACK_BOT_TOKEN"])
    global users_list
    update_users_list(True)
    global logger
    logger = logging.getLogger(config["logger_name"])
    global sleep_time
    sleep_time = 30
    global rate_limit
    rate_limit = 80
    global total_calls
    total_calls = 0
    global started_time
    started_time = datetime.now()
    global total_processed
    total_processed = 0

def main():
    init()
    convos = get_conversations(types="public_channel,private_channel,im,mpim")
    print_conversations_list(convos)
    # test = get_conversation_by_name("test", convos)
    # test_messages = get_conversation_history(test,
    #     inclusive=True,
    #     oldest=utils.dt_to_ts("2018-01-01 01:00:00"),
    #     limit=100000000)
    # print(len(test_messages))
    # print(test_messages[0].keys())
    # export_conversation_history(test, messages=test_messages)
    # export_all_conversations_history(convos, multi_threaded=False,
    #     oldest=utils.dt_to_ts("2018-01-01 01:00:00"),
    #     limit=100000000)

    export_all_conversations_members(convos, True)


if __name__ == "__main__":
    main()

