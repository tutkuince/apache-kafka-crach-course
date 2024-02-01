package io.tutkuince.peopleservice.command;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class CreatePeopleCommand {
    private int count;
}
