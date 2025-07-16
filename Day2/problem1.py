AUTHENTICATED = False

def require_auth(func):
    def verify_auth(*args,**kwargs):
        global AUTHENTICATED
        if AUTHENTICATED:
            return func(*args,**kwargs)
        else:
            return denied()
    return verify_auth

@require_auth
def view_dashboard():
    print("Welcome to your Dashboard")
    
def login():
    global AUTHENTICATED
    AUTHENTICATED = True
    print("Logged in")

def logout():
    global AUTHENTICATED
    AUTHENTICATED = False
    print("Logged out")

def denied():
    print("Access Denied")

view_dashboard()
login()
view_dashboard()
logout()
view_dashboard()

    