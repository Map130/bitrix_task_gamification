class User():
    def __init__(self, fullname: str, userid: int, tasks: list[dict]):
        self.fullname = fullname
        self.userid = userid
        self.complite_tasks = self.count_complite_tasks(tasks)

    def count_complite_tasks(self, tasks: list[dict]) -> int:
        return sum(1 for task in tasks if task['STATUS'] == '5')