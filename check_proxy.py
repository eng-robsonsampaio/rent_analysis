import threading
import queue

import requests

q = queue.Queue()

valid_proxies = []

with open('proxy_list.txt', 'r') as file:
    proxies = file.read().split('\n')
    for proxy in proxies:
        # print(proxy)
        q.put(proxy)

print("proxy list read")

    
def check_proxies():
    global q
    print("Start testing")
    while not q.empty():
        proxy = q.get()
        try:
            # print(proxy)
            # print("\n")
            res = requests.get("https://www.vivareal.com.br/aluguel/ceara/fortaleza/",
                            proxies={
                                "http": proxy, 
                                "https": proxy
                                })
        except:
            print("Err")
            continue

        if res.status_code == 200:
            print(f"{proxy} | OK")
        else:
            print(f"{proxy} | NOT OK")
        
# check_proxies()
for _ in range(10):
    threading.Thread(target=check_proxies).start()