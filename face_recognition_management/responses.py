from rest_framework.response import Response
from rest_framework import status

class ResponseCreated(Response):
    def __init__(self, data=None, message='created', status_code=status.HTTP_201_CREATED, headers=None, **kwargs):
        if data is None:
            data = {}
        formatted_data = self.format_data(data, message, status_code)
        super().__init__(data=formatted_data, status=status_code, headers=headers, **kwargs)
    
    def format_data(self, data, message, status_code):
        return {
            'code': status_code,
            'message': message,
            'data': data
        }

class ResponseOk(Response):
    def __init__(self, data=None, message='ok', status_code=status.HTTP_200_OK, headers=None, **kwargs):
        if data is None:
            data = {}
        formatted_data = self.format_data(data, message, status_code)
        super().__init__(data=formatted_data, status=status_code, headers=headers, **kwargs)
    
    def format_data(self, data, message, status_code):
        return {
            'code': status_code,
            'message': message,
            'data': data
        }
    
class ResponseNotFound(Response):
    def __init__(self, message='Not found', status_code=status.HTTP_404_NOT_FOUND, headers=None, **kwargs):
        formatted_data = self.format_data(message, status_code)
        super().__init__(data=formatted_data, status=status_code, headers=headers, **kwargs)
    
    def format_data(self, message, status_code):
        return {
            'code': status_code,
            'message': message,
            'data': {}
        }
    
class ResponseBadRequest(Response):
    def __init__(self, message='Bad Request', status_code=status.HTTP_400_BAD_REQUEST, headers=None, **kwargs):
        formatted_data = self.format_data(message, status_code)
        super().__init__(data=formatted_data, status=status_code, headers=headers, **kwargs)
    
    def format_data(self, message, status_code):
        return {
            'code': status_code,
            'message': message,
            'data': {}
        }
    
class ResponseForbidden(Response):
    def __init__(self, message='Forbidden', status_code=status.HTTP_403_FORBIDDEN, headers=None, **kwargs):
        formatted_data = self.format_data(message, status_code)
        super().__init__(data=formatted_data, status=status_code, headers=headers, **kwargs)
    
    def format_data(self, message, status_code):
        return {
            'code': status_code,
            'message': message,
            "data": {}
        }
    
class ResponseUnAuthorized(Response):
    def __init__(self, message='Unauthorized', status_code=status.HTTP_401_UNAUTHORIZED, headers=None, **kwargs):
        formatted_data = self.format_data(message, status_code)
        super().__init__(data=formatted_data, status=status_code, headers=headers, **kwargs)
    
    def format_data(self, message, status_code):
        return {
            'code': status_code,
            'message': message,
            "data": {}
        } 
    
class ResponseInternalServerError(Response):
    def __init__(self, message='Internal Server Error', status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, headers=None, **kwargs):
        formatted_data = self.format_data(message, status_code)
        super().__init__(data=formatted_data, status=status_code, headers=headers, **kwargs)
    
    def format_data(self, message, status_code):
        return {
            'code': status_code,
            'message': message,
            "data": {}
        }
