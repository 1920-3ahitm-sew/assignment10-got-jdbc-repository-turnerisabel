package at.htl.gotjdbcrepository.control;

import at.htl.gotjdbcrepository.entity.Person;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class PersonRepository implements Repository {
    public static final String USERNAME = "app";
    public static final String PASSWORD = "app";
    public static final String DATABASE = "db";
    public static final String URL = "jdbc:derby://localhost:1527/" + DATABASE + ";create=true";
    public static final String TABLE_NAME = "person";

    private static PersonRepository instance;

    private PersonRepository() {
        createTable();
    }

    public static synchronized PersonRepository getInstance() {
        if (instance == null){
            instance =  new PersonRepository();
        }
        return instance;
    }

    private void createTable() {
        try (Connection conn = DriverManager.getConnection(URL, USERNAME, PASSWORD)) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "CREATE TABLE " + TABLE_NAME + " (" +
                        "id INT NOT NULL GENERATED ALWAYS AS IDENTITY CONSTRAINT " + TABLE_NAME + "_pk PRIMARY KEY," +
                        "name VARCHAR(255)," +
                        "city VARCHAR(255)," +
                        "house VARCHAR(255)," +
                        "CONSTRAINT " + TABLE_NAME + "_uq UNIQUE (name, city, house)" +
                        ")";
                stmt.executeUpdate(sql);
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            //System.err.format("SQL State: %s - %s\n", e.getSQLState(), e.getMessage());
        }
    }

    public void deleteAll() {
        try(Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)){
            try(Statement statement = connection.createStatement()){
                statement.executeUpdate("DELETE FROM PERSON");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * Hat newPerson eine ID (id != null) so in der Tabelle die entsprechende Person gesucht und upgedated
     * Hat newPerson keine ID wird ein neuer Datensatz eingefügt.
     *
     * Wie man die generierte ID zurück erhält: https://stackoverflow.com/a/1915197
     *
     * Falls ein Fehler auftritt, wird nur die Fehlermeldung ausgegeben, der Programmlauf nicht abgebrochen
     *
     * Verwenden sie hier die privaten MEthoden update() und insert()
     *
     * @param newPerson
     * @return die gespeicherte Person mit der (neuen) id
     */
    @Override
    public Person save(Person newPerson) {
        if (newPerson.getId() != null){
            update(newPerson);
        }else {
            insert(newPerson);
        }
        return null;
    }

    /**
     *
     * Wie man die generierte ID erhält: https://stackoverflow.com/a/1915197
     *
     * @param personToSave
     * @return Rückgabe der Person inklusive der neu generierten ID
     */
    private Person insert(Person personToSave) {
        try(Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)){
            try(PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO PERSON (name, city, house) VALUES (?, ?, ?)")){
                preparedStatement.setString(1, personToSave.getName());
                preparedStatement.setString(2, personToSave.getCity());
                preparedStatement.setString(3, personToSave.getHouse());
                preparedStatement.executeUpdate();
            }
        } catch (SQLException e) {
            System.err.println();
        }
        return null;
    }

    /**
     *
     * @param personToSave
     * @return wenn erfolgreich --> Anzahl der eingefügten Zeilen, also 1
     *         wenn nicht erfolgreich --> -1
     */
    private int update(Person personToSave) {
        try(Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)){
            try(PreparedStatement preparedStatement = connection.prepareStatement("UPDATE PERSON" +
                    "SET NAME = ?, city = ?, house = ? WHERE id = ?")){
                preparedStatement.setString(1, personToSave.getName());
                preparedStatement.setString(2, personToSave.getCity());
                preparedStatement.setString(3, personToSave.getHouse());
                preparedStatement.setLong(4, personToSave.getId());
                preparedStatement.executeUpdate();
                return 1;
            }
        } catch (SQLException e) {
            System.err.println();
        }
        return -1;
    }

    @Override
    public void delete(long id) {
        try(Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)){
            try(Statement statement = connection.createStatement()){
                statement.executeUpdate("DELETE FROM PERSON WHERE ID = " + id);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * Finden Sie eine Person anhand Ihrer ID
     *
     * @param id
     * @return die gefundene Person oder wenn nicht gefunden wird null zurückgegeben
     */
    public Person find(long id) {
        try(Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)){
            try(Statement statement = connection.createStatement()){
                ResultSet resultSet = statement.executeQuery("SELECT * FROM PERSON WHERE ID = " + id);
                Person selectedPerson = new Person();
                selectedPerson.setId(id);
                resultSet.next();
                selectedPerson.setName(resultSet.getString("name"));
                selectedPerson.setCity(resultSet.getString("city"));
                selectedPerson.setHouse(resultSet.getString("house"));
                return selectedPerson;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *
     * @param house Name des Hauses
     * @return Liste aller Personen des gegebenen Hauses
     */
    public List<Person> findByHouse(String house) {
        List<Person> personList = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("SELECT * FROM PERSON WHERE HOUSE = " + house);
                Person selectedPerson = new Person();
                selectedPerson.setHouse(house);
                resultSet.next();
                selectedPerson.setId(resultSet.getLong("id" ));
                selectedPerson.setName(resultSet.getString("name" ));
                selectedPerson.setCity(resultSet.getString("city" ));
                personList.add(selectedPerson);
                return personList;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}