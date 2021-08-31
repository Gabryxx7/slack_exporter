from slack_exporter import SlackExporter

if __name__ == "__main__":
    members_list = {}
    exporter = SlackExporter("config.yaml")
    convos = exporter.get_conversations(types="public_channel,private_channel,im,mpim")
    exporter.print_conversations_list(convos)
    exporter.export_all_conversations_members(convos, True)
    for idx, convo in enumerate(exporter.get_conversations()):
        try:
            if convo['is_group'] and convo['is_private'] and not convo['is_mpim']:
                print(f"{idx+1} - {convo['id']}\t{convo['name']}\t")
                conversation = {"id":convo['id']}
                res = exporter.get_conversation_members(conversation)
                for user in res:
                    members_list[user['id']] = {"name":user['profile']['real_name_normalized'], "username":user['name']}
                    # print(f"{user['id']}\t{user['profile']['real_name_normalized']}\t{user['name']}\t")
        except Exception as e:
            # print(convo.keys())
            pass
        
    for key, val in members_list.items():
        print(f"{key}\t{val['username']}\t{val['name']}\t")
    # exporter.get_conversations_generic()
# conversations = {"id":"ABCDEF", "name":"blabla"}
# res = exporter.get_conversation_members(conversation)
# for user in res:
#     print(f"{user['id']}\t{user['profile']['real_name_normalized']}\t{user['name']}\t")
    # for user in res:
    #     print(f"{user['profile']['real_name_normalized']}")
    
