# Role

1. Super - Full
2. Admin
3. Host - View History, delete employee
4. Employee - View history

# Command line

- aws rekognition list-collections
- aws rekognition list-faces --collection-id employees
- aws rekognition delete-faces --collection-id employees --face-ids 767d3077-6fc9-4eb8-bec4-750e0e5a9709

# History Model

- id = device_id
- employee_information: id, name, image,
- created_at
- updated_at

# Black List Model

- token
- user_id
- created_at
- updated_at
- reason

# History Action

- device_id_user_id
- action
- created_at
- updated_at

Action: TURN_ON_LED, TURN_OFF_LED, TURN_ON_FAN, TURN_OFF_FAN
