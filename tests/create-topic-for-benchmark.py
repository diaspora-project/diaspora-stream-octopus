from diaspora_stream.api import Driver

driver = Driver.new("octopus", options={"bootstrap.servers":"localhost:9092"})
driver.create_topic("my_topic", options={"replication_factor":1})
