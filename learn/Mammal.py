class Mammal:
    def walk(self):
        print("walk")


#Dog 继承了Mammal类
class Dog(Mammal):
    #空类要加pass
    pass


class Cat(Mammal):
    def bark(self):
        print("fish")