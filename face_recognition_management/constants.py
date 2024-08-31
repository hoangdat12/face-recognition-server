from enum import Enum

class DeviceStatus(Enum): 
    ACTIVE = 'active'
    INACTIVE = 'inactive'
    PENDING = 'pending'

class UserAccountStatus(Enum):
    ACTIVE = 'active'
    INACTIVE = 'inactive'
    DELETED = 'deleted'

class Role(Enum):
    SUPER = 'super'
    ADMIN = 'admin'
    HOST = 'host'
    EMPLOYEE = 'employee'

class Prefix(Enum):
    REKOGNITION_COLLECTION_PREFIX = 'employee'
    S3_GUEST_BUCKET_PREFIX = 'recognition-employees'
    S3_EMPLOYEE_BUCKET_PREFIX = 'recognition-guest'

class AuthenticateMethod(Enum):
    FACE_RECOGNITION='face'
    ACCOUNT='account'

class BlackListReson(Enum):
    LOGOUT='logout'
    REFRESH='refresh'
    TOKEN_COMPROMISED='toke_compromised'
    BLOCK_ACCOUNT = 'block'
    EXPIRED = 'expired'

class HistoryAction(Enum):
    TURN_ON_LED = 'Turn on Led',
    TURN_OFF_LED = 'Turn off Led',
    TURN_ON_FAN = 'Turn on fan',
    TURN_OFF_FAN = 'Turn off fan'
