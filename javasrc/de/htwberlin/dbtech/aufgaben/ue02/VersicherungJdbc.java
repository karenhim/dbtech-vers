package de.htwberlin.dbtech.aufgaben.ue02;

import de.htwberlin.dbtech.exceptions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Use try-with-resources for PreparedStatement and ResultSet
 * Get the connection using a separate method (useConnection())
 * Use logging (L.info, L.warn, L.error) for readibility
 * Convert SQL types (like Date) carefully to/from Java types (LocalDate)
 */

public class VersicherungJdbc implements IVersicherungJdbc {
    private static final Logger L = LoggerFactory.getLogger(VersicherungJdbc.class);
    private Connection connection; // Diese Verbindung wird von außen gesetzt und verwaltet

    @Override
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    private Connection useConnection() {
        if (connection == null) {
            L.error("Connection not set before use.");
            throw new DataException("Connection not set");
        }
         return connection;
    }

    /**
     * Returns a list of product short names from the Produkt table
     * List is sorted by ID
     * @return
     */

    @Override
    public List<String> kurzBezProdukte() {
        L.info("kurzBezProdukte: start");
        List<String> kurzBezeichnungen = new ArrayList<>(); //return Wert
        String sql = "SELECT KurzBez FROM Produkt ORDER BY ID"; // implementation!
        Connection conn = useConnection(); // Verbindung holen, aber nicht im try-with-resources

        try (PreparedStatement pstmt = conn.prepareStatement(sql); // alle Values/Werte sammeln
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                kurzBezeichnungen.add(rs.getString("KurzBez"));
            }
        } catch (SQLException e) {
            L.error("Error fetching Produkt KurzBezeichnungen", e);
            throw new DataException("Datenbankfehler beim Laden der Produktbezeichnungen.", e);
        }
        L.info("kurzBezProdukte: ende, anzahl={}", kurzBezeichnungen.size());
        return kurzBezeichnungen;
    }

    /**
     * @param id Primaerschuessel des Kunden.
     * @return Kunde object with correct Name and Geburtsdatum (if found)
     * Otherwise, throw KundeExistiertNichtException
     */
    @Override
    public Kunde findKundeById(Integer id) {
        L.info("findKundeById: start, id={}", id);
        String sql = "SELECT Name, Geburtsdatum FROM Kunde WHERE ID = ?"; // implementation!
        Kunde kunde = null;
        Connection conn = useConnection();

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, id);  //select ID as parameter
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) { //check is the result exists
                    String name = rs.getString("Name");
                    LocalDate geburtsdatum = rs.getDate("Geburtsdatum").toLocalDate();
                    //converting java.sql.date to LocalDate() (avoid confusion with time or timezone)
                    kunde = new Kunde(id, name, geburtsdatum);
                } else {
                    L.warn("Kunde mit ID {} nicht gefunden.", id);
                    throw new KundeExistiertNichtException(id);
                }
            }
        } catch (KundeExistiertNichtException e) {
            throw e;
        } catch (SQLException e) {
            L.error("Error finding Kunde by ID " + id, e);
            throw new DataException("Datenbankfehler beim Suchen von Kunde mit ID " + id, e);
        }
        L.info("findKundeById: ende, kundeGefunden={}", kunde != null);
        return kunde;
    }

    /**
     * Helper methods for createVertrag tests
     * entityExists prevents repeating code
     * & is reusable for 3 checks (Kunde, Produkt, Vertrag)
     * @param tableName
     * @param idColumnName
     * @param id
     * @return
     * @throws SQLException
     */
    private boolean entityExists(String tableName, String idColumnName, Integer id) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + tableName + " WHERE " + idColumnName + " = ?";
        Connection conn = useConnection();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, id);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    private boolean kundeExistiert(Integer kundenId) throws SQLException {
        return entityExists("Kunde", "ID", kundenId);
    }

    private boolean produktExistiert(Integer produktId) throws SQLException {
        return entityExists("Produkt", "ID", produktId);
    }

    private boolean vertragExistiert(Integer vertragsId) throws SQLException {
        return entityExists("Vertrag", "ID", vertragsId);
    }


    @Override
    public void createVertrag(Integer id, Integer produktId, Integer kundenId, LocalDate versicherungsbeginn) {
        L.info("createVertrag: start, id={}, produktId={}, kundenId={}, beginn={}", id, produktId, kundenId, versicherungsbeginn);
        /*
        SQL Befehl:
        INSERT INTO Vertrag (ID, Produkt_FK, Kunde_FK, Versicherungsbeginn, Versicherungsende)
        VALUES (?, ?, ?, ?, ?)
         */

        Connection conn = useConnection(); // Verbindung einmal holen für diese Methode

        try { //"Create a new insurance contract only if..."
            if (versicherungsbeginn.isBefore(LocalDate.now())) { //Use LocalDate.now() to compare the input start date
                L.warn("Versuch, Vertrag mit Datum in Vergangenheit zu erstellen: {}", versicherungsbeginn);
                throw new DatumInVergangenheitException(versicherungsbeginn);
            }

            // Existenzprüfungen verwenden die bereits geholte `conn` implizit durch `useConnection()`

            if (!produktExistiert(produktId)) {
                L.warn("Produkt mit ID {} existiert nicht.", produktId);
                throw new ProduktExistiertNichtException(produktId); //throws exception for non-existing contracts
            }

            if (!kundeExistiert(kundenId)) {
                L.warn("Kunde mit ID {} existiert nicht.", kundenId);
                throw new KundeExistiertNichtException(kundenId); //throws exception for non-existing customers
            }

            if (vertragExistiert(id)) {
                L.warn("Vertrag mit ID {} existiert bereits.", id);
                throw new VertragExistiertBereitsException(id); //throws exception for non-existing contract ID
            }

            /**
             * Calculate Versicherungsende = Versicherungsbeginn + 1Y - 1D
             */
            LocalDate versicherungsende = versicherungsbeginn.plusYears(1).minusDays(1);
            String sql = "INSERT INTO Vertrag (ID, Produkt_FK, Kunde_FK, Versicherungsbeginn, Versicherungsende) VALUES (?, ?, ?, ?, ?)";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) { // Hier die geholte `conn` verwenden
                pstmt.setInt(1, id);
                pstmt.setInt(2, produktId);
                pstmt.setInt(3, kundenId);
                pstmt.setDate(4, Date.valueOf(versicherungsbeginn)); //Use Date.valueOf() for converting LocalDate to java.sql.Date
                pstmt.setDate(5, Date.valueOf(versicherungsende));

                int affectedRows = pstmt.executeUpdate();
                if (affectedRows == 0) {
                    L.error("Vertrag konnte nicht erstellt werden, keine Zeile betroffen, ID: {}", id);
                    throw new DataException("Vertrag konnte nicht erstellt werden für ID: " + id + " (executeUpdate lieferte 0).");
                }
                L.info("Vertrag mit ID {} erfolgreich erstellt.", id);
            }

        } catch (DatumInVergangenheitException | ProduktExistiertNichtException | KundeExistiertNichtException | VertragExistiertBereitsException e) {
            throw e;
        } catch (SQLException e) {
            L.error("Datenbankfehler bei createVertrag für ID " + id, e);
            throw new DataException("Datenbankfehler bei createVertrag für ID " + id, e);
        }
        L.info("createVertrag: ende");
    }

    /**
     * Calculates the correct price based on the coverage amounts and validity periods
     * @param vertragsId Primaerschuessel des Vertrags.
     * @return
     */
    @Override
    public BigDecimal calcMonatsrate(Integer vertragsId) {
        L.info("calcMonatsrate: start, vertragsId={}", vertragsId);
        BigDecimal monatsrate = BigDecimal.ZERO;
        LocalDate versicherungsbeginn;
        Connection conn = useConnection(); // Verbindung einmal holen

        String vertragSql = "SELECT Versicherungsbeginn FROM Vertrag WHERE ID = ?";
        try (PreparedStatement pstmtVertrag = conn.prepareStatement(vertragSql)) {
            pstmtVertrag.setInt(1, vertragsId);
            try (ResultSet rsVertrag = pstmtVertrag.executeQuery()) {
                if (rsVertrag.next()) {
                    versicherungsbeginn = rsVertrag.getDate("Versicherungsbeginn").toLocalDate();
                } else {
                    L.warn("Vertrag mit ID {} für Ratenberechnung nicht gefunden.", vertragsId);
                    throw new VertragExistiertNichtException(vertragsId); //if get Verischerungsbeginn not found
                }
            }
        } catch (VertragExistiertNichtException e) {
            throw e;
        } catch (SQLException e) {
            L.error("DB Fehler beim Holen des Versicherungsbeginns für Vertrag ID " + vertragsId, e);
            throw new DataException("DB Fehler beim Holen des Versicherungsbeginns für Vertrag ID " + vertragsId, e);
        }

        // JOIN the tables Deckung, Deckungsbetrag, Deckungspreis

        // SQL Query:
        String preisSql = "SELECT SUM(dp.Preis) AS Gesamtpreis " +
                // SUM (dp.Preis) for all matching rows...→ If no rows found, sum = 0
                "FROM Deckung d " +
                "JOIN Deckungsbetrag db ON d.Deckungsart_FK = db.Deckungsart_FK AND d.Deckungsbetrag = db.Deckungsbetrag " +
                "JOIN Deckungspreis dp ON db.ID = dp.Deckungsbetrag_FK " +
                "WHERE d.Vertrag_FK = ? " +
                "AND ? >= dp.Gueltig_Von " + //Filter Deckungspreis rows
                "AND ? <= dp.Gueltig_Bis";
                // prices should be valid between Gueltig_Von and Gueltig_Bis

        try (PreparedStatement pstmtPreis = conn.prepareStatement(preisSql)) {
            pstmtPreis.setInt(1, vertragsId);
            pstmtPreis.setDate(2, Date.valueOf(versicherungsbeginn));
            pstmtPreis.setDate(3, Date.valueOf(versicherungsbeginn));

            try (ResultSet rsPreis = pstmtPreis.executeQuery()) {
                if (rsPreis.next()) {
                    BigDecimal summe = rsPreis.getBigDecimal("Gesamtpreis");
                    if (summe != null) {
                        monatsrate = summe;
                    }
                }
            }
        } catch (SQLException e) {
            L.error("DB Fehler bei der Berechnung der Monatsrate für Vertrag ID " + vertragsId, e);
            throw new DataException("DB Fehler bei der Berechnung der Monatsrate für Vertrag ID " + vertragsId, e);
        }

        L.info("calcMonatsrate: ende, vertragsId={}, monatsrate={}", vertragsId, monatsrate);
        return monatsrate; //Return als BigDecimal
    }
}