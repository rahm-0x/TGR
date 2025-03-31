import json

with open("thegrowersresource-1f2d7-firebase-adminsdk-hj18n-7101b02dc4.json") as f:
    firebase_json = json.load(f)

print('''"""{}"""'''.format(firebase_json["private_key"]))
