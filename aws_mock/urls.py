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
    path('v1/registerjobdefinition', views.register_job_definition_nf),
    path('v1/describejobdefinitions', views.describe_job_definition),
    path('v1/describejobs', views.describe_jobs),
    path('v1/listjobs', views.list_jobs),
    path('v1/terminatejob', views.terminate_job),
    path('jobs/<uuid:root_id>/<str:job_name>/<str:file_name>', views.upload_s3),
    path('test/', views.return_200),
    path('', views.return_xml),
    path('wes_samples/<str:dir>/<str:name>', views.acl),
    path('test/<str:dir>/<str:job_id>/', views.put_directory),
    path('test/<str:dir>/<str:job_id>/<str:file_name>', views.put_file),
    #path('jobs/<uuid:root_id>/<str:job_name>/<uuid:job_id>/<str:file_name>', views.download_s3),
    path('jobs/<str:root_dir>/<str:job_name>/<str:job_id>/<str:file_name>/', views.download_s3),

]
