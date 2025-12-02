
try:
    from langsmith import Client
    import inspect
    
    client = Client()
    print("Client methods:")
    for name, method in inspect.getmembers(client):
        if not name.startswith('_'):
            print(f"- {name}")
            
except Exception as e:
    print(f"Error: {e}")
