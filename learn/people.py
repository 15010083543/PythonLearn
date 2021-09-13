class People:
    def __init__(self, name):
        self.name = name

    def talk(self):
        print(f"hello word{self.name}")


people = People("liu")
print(people.name)
people.name = "li"
print(people.name)
people.talk()