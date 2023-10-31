class Task:
    def __init__(self, name, func, priority):
        self.name = name
        self.func = func
        self.priority = priority


class Executor:
    def __init__(self):
        self.tasks = []

    def add_task(self, task):
        self.tasks.append(task)

    def run(self):
        sorted_task = sorted(self.tasks, key=lambda x: x.priority)
        for task in sorted_task:
            print("Running Task: {}".format(task.name))
            task.func


if __name__ == '__main__':
    def task1():
        print("Task 1 Executed")


    def task2():
        print("Task 2 Executed")


    def task3():
        print("Task 3 Executed")


    task_1 = Task("Task_1", task1(), priority=3)
    task_2 = Task("Task_2", task2(), priority=2)
    task_3 = Task("Task_3", task3(), priority=1)
    executor = Executor()
    executor.add_task(task_1)
    executor.add_task(task_2)
    executor.add_task(task_3)
    executor.run()
