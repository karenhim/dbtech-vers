package de.htwberlin.dbtech.aufgaben.ue02;


/*
  @author Ingo Classen
 */

import de.htwberlin.dbtech.exceptions.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * VersicherungJdbc
 */
public class VersicherungJdbc implements IVersicherungJdbc {
    private static final Logger L = LoggerFactory.getLogger(VersicherungJdbc.class);
    private Connection connection; // This connection is set and managed externally

    @Override
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    @SuppressWarnings("unused")
    private Connection useConnection() {
        if (connection == null) {
            L.error("Connection not set before use.");
            throw new DataException("Connection not set");
        }
        return connection;
    }
    // No check for isClosed() here, as this generates overhead with every call.
    // The calling method must be able to handle SQLExceptions (such as “closed connection”).

    //this method will run a list of short product names
    @Override
    public List<String> kurzBezProdukte() {
        L.info("kurzBezProdukte: start");  //logging a message saying the method has started
        List<String> kurzBezeichnungen = new ArrayList<>(); //create a new list
        String sql = "SELECT KurzBez FROM Produkt ORDER BY ID"; //write an sql query
        Connection conn = useConnection(); // Get the connection to the database (but don't close it automatically inside try).

        try (PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            //Prepare and execute the SQL query. The try block will automatically close pstmt and rs when done.

            while (rs.next()) {
                kurzBezeichnungen.add(rs.getString("KurzBez"));
            }
            // Go through each row in the result set and add the value of KurzBez to the list.
        } catch (SQLException e) {
            L.error("Error fetching Produkt KurzBezeichnungen", e);
            throw new DataException("Datenbankfehler beim Laden der Produktbezeichnungen.", e);
            // If something goes wrong with the database, log the error and throw a custom exception.
        }
        L.info("kurzBezProdukte: ende, anzahl={}", kurzBezeichnungen.size());
        // Log how many product names were found and that the method is done.

        return kurzBezeichnungen;
        // Return the list of short product names.
    }

    //select customer info from Kunde table
    @Override
    public Kunde findKundeById(Integer id) {
        L.info("id: " + id);
        L.info("ende");
        return null;
    }

    //insert a new row into Vertrag table
    @Override
    public void createVertrag(Integer id, Integer produktId, Integer kundenId, LocalDate versicherungsbeginn) {
        L.info("id: " + id);
        L.info("produktId: " + produktId);
        L.info("kundenId: " + kundenId);
        L.info("versicherungsbeginn: " + versicherungsbeginn);
        L.info("ende");
    }

    //calculate monthly rate (probably involves Deckung, Deckungsbetrag, and Deckungspreis)
    @Override
    public BigDecimal calcMonatsrate(Integer vertragsId) {
        L.info("vertragsId: " + vertragsId);

        L.info("ende");
        return null;
    }

}