from django.shortcuts import render, redirect
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
import json
from .db_backend import DBManager


def index(request):
    """Main page - render the todo list"""
    return render(request, 'todos/index.html')


def api_todos_list(request):
    """API endpoint to get all todos"""
    if request.method == 'GET':
        db = DBManager()
        todos = db.get_all_todos()
        return JsonResponse({'todos': todos})
    
    elif request.method == 'POST':
        try:
            data = json.loads(request.body)
            title = data.get('title', '').strip()
            description = data.get('description', '').strip()
            
            if not title:
                return JsonResponse({'error': 'Title is required'}, status=400)
            
            db = DBManager()
            todo = db.create_todo(title, description)
            
            if todo:
                return JsonResponse({'todo': todo}, status=201)
            else:
                return JsonResponse({'error': 'Failed to create todo'}, status=500)
        
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)


@csrf_exempt
@require_http_methods(["PUT", "DELETE"])
def api_todo_detail(request, todo_id):
    """API endpoint for a specific todo"""
    db = DBManager()
    
    if request.method == 'PUT':
        try:
            data = json.loads(request.body)
            
            success = db.update_todo(
                todo_id,
                title=data.get('title'),
                description=data.get('description'),
                completed=data.get('completed')
            )
            
            if success:
                todo = db.get_todo(todo_id)
                return JsonResponse({'todo': todo})
            else:
                return JsonResponse({'error': 'Failed to update todo'}, status=500)
        
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)
    
    elif request.method == 'DELETE':
        try:
            success = db.delete_todo(todo_id)
            
            if success:
                return JsonResponse({'message': 'Todo deleted successfully'})
            else:
                return JsonResponse({'error': 'Failed to delete todo'}, status=500)
        
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)


@csrf_exempt
def api_todos_list_csrf_exempt(request):
    """CSRF-exempt version for POST requests"""
    return api_todos_list(request)
