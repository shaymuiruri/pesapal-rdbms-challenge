from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('api/todos/', views.api_todos_list_csrf_exempt, name='api_todos_list'),
    path('api/todos/<int:todo_id>/', views.api_todo_detail, name='api_todo_detail'),
]