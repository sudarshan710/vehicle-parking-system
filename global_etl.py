import sqlite3
import time

g_no_of_entries = 0
max_entries = 7

def extract():
    print('extract')
    pass

def transform():
    print('transform')
    pass

def load():
    print('load')
    pass

def main():
    global g_no_of_entries
    global max_entries

    

    while g_no_of_entries < max_entries:

        start_time = time.time()

        print(f"Counter no: {g_no_of_entries}")
        extract()
        transform()
        load()
        g_no_of_entries += 1

        elapsed_time = time.time() - start_time
        wait_time = 5 - int(elapsed_time)
        if wait_time > 0:
            time.sleep(wait_time)
        

if __name__ == "__main__":
    main()
