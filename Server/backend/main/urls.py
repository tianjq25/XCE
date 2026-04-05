from django.urls import path
from . import views

urlpatterns = [
    path('', views.hello_world, name='hello_world'),
    path('api/table-data/', views.get_table_data, name='get_table_data'),
    path('api/job-join/', views.job_join, name='job_join'),
    path('api/decision-tree/models/', views.decision_tree_models, name='dt_models'),
    path('api/decision-tree/data/', views.decision_tree_data, name='dt_data'),
]
