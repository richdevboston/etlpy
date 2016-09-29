class A(object):
    def __init__(self):
        print('Enter A')
        self.__test = 0
class C(object):
    def __init__(self):
        print('Enter C')
        super(C,self).__init__()
        self.test2 = 3
class D(object):
    def __init__(self):
        print('enter D')
        self.test4=3
class B(A):
    def __init__(self):
        print('Enter B')
        super(B,self).__init__()
    def set(self):
        self.__test = 2
    def display(self):
        print(dir(self))
class E(C,D):
    def __init__(self):
        print('Enter E')
        super(E,self).__init__();


e=E()
print e.test2
print e.test4

