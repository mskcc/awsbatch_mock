"""aws_mock URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.1/topics/http/urls/
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
from django.contrib import admin
from django.urls import path
from django.contrib import admin
from django.urls import path
from django.http import JsonResponse
from batch import views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('v1/submitjob', views.submit_job),
    path('v1/registerjobdefinition', views.register_job_definition),
    path('v1/listjobs', views.list_jobs),
    path('jobs/<uuid:root_id>/<str:job_name>/<str:file_name>', views.upload_s3),
    path('jobs/<uuid:root_id>/<str:job_name>/<uuid:job_id>/<str:file_name>', views.download_s3),
]
