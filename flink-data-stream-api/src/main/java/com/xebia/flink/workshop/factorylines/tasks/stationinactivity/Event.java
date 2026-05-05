package com.xebia.flink.workshop.factorylines.tasks.stationinactivity;

import java.time.Instant;
import java.util.Objects;

public class Event {

    private Long id;
    private Instant timestamp;
    private String value;
    private String firstName;
    private String secondName;
    private String country;
    private String city;
    private String occupation;


    public Event() {
    }

    public Event(Long id, Instant timestamp, String value, String firstName, String secondName, String country, String city, String occupation) {
        this.id = id;
        this.timestamp = timestamp;
        this.city = city;
        this.country = country;
        this.firstName = firstName;
        this.occupation = occupation;
        this.secondName = secondName;
        this.value = value;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getOccupation() {
        return occupation;
    }

    public void setOccupation(String occupation) {
        this.occupation = occupation;
    }

    public String getSecondName() {
        return secondName;
    }

    public void setSecondName(String secondName) {
        this.secondName = secondName;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(id, event.id) && Objects.equals(timestamp, event.timestamp) && Objects.equals(value, event.value) && Objects.equals(firstName, event.firstName) && Objects.equals(secondName, event.secondName) && Objects.equals(country, event.country) && Objects.equals(city, event.city) && Objects.equals(occupation, event.occupation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, timestamp, value, firstName, secondName, country, city, occupation);
    }

    @Override
    public String toString() {
        return "Event{" +
                "id=" + id +
                ", timestamp=" + timestamp +
                ", value='" + value + '\'' +
                ", firstName='" + firstName + '\'' +
                ", secondName='" + secondName + '\'' +
                ", country='" + country + '\'' +
                ", city='" + city + '\'' +
                ", occupation='" + occupation + '\'' +
                '}';
    }
}
