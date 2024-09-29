require "kafka"

class KafkaConsumerService
  def initialize
    @kafka = Kafka.new([ "localhost:9092" ], client_id: "person-event-backend")
    @consumer = @kafka.consumer(group_id: "person-event-backend-group")
    @consumer.subscribe("petition")
  end

  def run
    @consumer.each_message do |message|
      process_message(JSON.parse(message.value))
    end
  end

  private

  def process_message(petition)
    case petition["resource"]
    when "person"
      process_person_petition(petition)
    when "event"
      process_event_petition(petition)
    else
      respond_to_petition(petition, { error: "Unknown resource" })
    end
  end

  def process_person_petition(petition)
    case petition["action"]
    when "index"
      people = Person.all
      respond_to_petition(petition, people)
    when "show"
      person = Person.find_by(id: petition["data"]["person_id"])
      respond_to_petition(petition, person || { error: "Person not found" })
    when "create"
      person = Person.create(petition["data"])
      respond_to_petition(petition, person)
    when "update"
      person = Person.find_by(id: petition["data"]["id"])
      if person&.update(petition["data"])
        respond_to_petition(petition, person)
      else
        respond_to_petition(petition, { error: "Person update failed" })
      end
    when "destroy"
      person = Person.find_by(id: petition["data"]["id"])
      if person&.destroy
        respond_to_petition(petition, { message: "Person deleted" })
      else
        respond_to_petition(petition, { error: "Person deletion failed" })
      end
    end
  end

  def process_event_petition(petition)
    case petition["action"]
    when "index"
      events = Event.all
      respond_to_petition(petition, events)
    when "show"
      event = Event.find_by(id: petition["data"]["event_id"])
      respond_to_petition(petition, event || { error: "Event not found" })
    when "create"
      event = Event.create(petition["data"])
      respond_to_petition(petition, event)
    when "update"
      event = Event.find_by(id: petition["data"]["id"])
      if event&.update(petition["data"])
        respond_to_petition(petition, event)
      else
        respond_to_petition(petition, { error: "Event update failed" })
      end
    when "destroy"
      event = Event.find_by(id: petition["data"]["id"])
      if event&.destroy
        respond_to_petition(petition, { message: "Event deleted" })
      else
        respond_to_petition(petition, { error: "Event deletion failed" })
      end
    end
  end

  def respond_to_petition(petition, response_data)
    producer = @kafka.producer
    response = { petition_id: petition["id"], data: response_data }
    producer.produce(response.to_json, topic: "response")
    producer.deliver_messages
  end
end
