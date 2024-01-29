# fastAPI-raft
Raft consensus algorithm powered by fastAPI endpoints
How to start:
1. Clone the repo
2. Run python -m uvicorn main:app --reload --port x on y different terminals, (in this case, I have configured y= 4 nodes on ports 8011,8012,8013,8014, so four different terminals are needed, but any amount should work, given it is configured in the main.py)
3. For demonstration and debugging, use /docs endpoint
