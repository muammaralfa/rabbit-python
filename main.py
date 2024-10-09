from services.consumer import Consumer


class Main:
    def __init__(self):
        self.consumer = Consumer()

    def run(self):
        self.consumer.consume()


if __name__ == "__main__":
    app = Main()
    app.run()
