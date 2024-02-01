package io.tutkuince.peopleservice.entity;

import lombok.*;

@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Person {
    private String id;
    private String name;
    private String title;
}
