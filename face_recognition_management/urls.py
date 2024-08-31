"""
URL configuration for face_recognition_server project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.urls import path
from . import views

urlpatterns = [
    #Token
    path('token/refresh', views.generate_new_at, name="Generate_new_access_token"),

    # face
    path('face/registor/host', views.registor_master_account, name="registor_master_account"),
    path('face/registor/employee', views.registration_employees, name="registor_employees"),
    path('face/authenticate', views.authenticate_employees, name="authenticate_employees"),

    # account
    path('account/authenticate', views.authenticate_account, name="authenticate_account"),
    path('account/<str:employee_id>', views.disable_employee_in_device, name="disable_employee_in_device"),
    path('account/detail/<str:user_id>', views.get_user_information, name="get_user_information"),
    path('account/update/<str:user_id>', views.update_account_information, name="update_account_information"),
    path('account/update/avatar/<str:user_id>', views.update_account_avatar, name="update_account_avatar"),

    # device
    path('device/generate', views.generate_device_id, name="generate_device_id"),
    path('device/employee/<str:device_id>', views.get_employee_in_device, name="get_employee_in_device"),
    path('device/<str:device_id>', views.get_device_detail, name="get_device_detail"),
    path('device/<str:device_id>', views.disable_device, name="disable_device"),
    path('device/update/<str:device_id>', views.update_device_information, name="update_device_information"),

    # history
    # path('history/test', views.generate_data, name="generate_data"),
    path('history/date/<str:device_id>', views.get_history_by_date, name="get_history_by_date"),
    path('history/<str:device_id>', views.get_history, name="get_history"),

    # history action
    path('history/action/create', views.create_history_action, name="create_history_action"),
    path('history/action/variables', views.get_history_type, name="get_history_type"),
    path('history/user/action', views.get_history_action, name="get_history_action"),

    # Heal-check
    path('', views.hello_server, name="hello_server"),
]
