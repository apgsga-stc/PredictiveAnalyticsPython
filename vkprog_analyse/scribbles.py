print("Hi, this is scribbles!")
print("Bye")

a=2
b= a-1



import pandas as pd

def greeting(name: str) -> str:
    return 'Hello, {}'.format(name)

this_is_a_list = [1,2,3]

greeting(name=this_is_a_list)

greeting(name="kartoffel")

########
import random
def do_you_get_it(blah: pd.DataFrame) -> pd.DataFrame:
    if 2==random.randint(1,5):
        return 5
    else:
        return blah


greeting(do_you_get_it("test"))


isinstance()
