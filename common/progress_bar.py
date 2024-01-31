def update_progress_bar(index, length):
    print('Progress: |', end="")
    for i in range(length):
        if i <= index:
            print('#', end="")
        else:
            print('-', end="")
    print(f'| ({str(index + 1).zfill(2)}/{length})')