package io.tutkuince.peopleservice.controller;

import com.github.javafaker.Faker;
import io.tutkuince.peopleservice.command.CreatePeopleCommand;
import io.tutkuince.peopleservice.entity.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api")
public class PeopleController {

    static final Logger logger = LoggerFactory.getLogger(PeopleController.class);

    @Value("${topics.people-basic.name}")
    String peopleTopic;

    private KafkaTemplate<String, Person> kafkaTemplate;

    public PeopleController(KafkaTemplate<String, Person> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/people")
    @ResponseStatus(HttpStatus.CREATED)
    public List<Person> create(@RequestBody CreatePeopleCommand createPeopleCommand) {
        logger.info("CreateCommand is {}", createPeopleCommand);

        Faker faker = new Faker();
        List<Person> peopleList = new ArrayList<>();
        for (int i = 0; i < createPeopleCommand.getCount(); i++) {
            Person person = Person.builder()
                    .id(UUID.randomUUID().toString())
                    .name(faker.name().fullName())
                    .title(faker.job().title())
                    .build();
            peopleList.add(person);

            CompletableFuture<SendResult<String, Person>> future = kafkaTemplate.send(
                    peopleTopic,
                    person.getTitle().toLowerCase().replaceAll("\\s+", "-"), person
            );
        }

        kafkaTemplate.flush();
        return peopleList;
    }
}
