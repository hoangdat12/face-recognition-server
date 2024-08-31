from rest_framework.decorators import api_view
from ..repository import UserRepository
from ..services import TokenService
from rest_framework.decorators import api_view
from ..responses import *

"""
Generate new JWT when Access Token is expired
"""
@api_view(['POST'])
def generate_new_at(request):
    refresh_token = request.POST.get('refreshToken');
    if not refresh_token:
        return ResponseBadRequest(message="Missing refresh token")

    # Verify token
    token_service = TokenService()
    verify_token_response = token_service.verify_refresh_token(refresh_token)
    if not verify_token_response["isSuccess"]:
        return ResponseUnAuthorized(message=verify_token_response["payload"])
    
    # Get payload
    payload = verify_token_response['payload']

    found_user = UserRepository.find_active_user_by_id(payload["id"]);
    if not found_user:
        return ResponseUnAuthorized(message="User not found")
    
    token_pairs = token_service.generate(found_user);

    # add older token into blackblist

    return ResponseOk(data=token_pairs)