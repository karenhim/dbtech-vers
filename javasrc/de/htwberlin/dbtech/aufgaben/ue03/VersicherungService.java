package de.htwberlin.dbtech.aufgaben.ue03;

import de.htwberlin.dbtech.exceptions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VersicherungService implements IVersicherungService {
    private static final Logger L = LoggerFactory.getLogger(VersicherungService.class);
    private Connection connection;

    // Struktur für Rückgabe von Vertragsdaten
    private static class Vertragsdaten {
        final int produktFk;
        final int kundeFk;
        final LocalDate versicherungsbeginn;

        Vertragsdaten(int produktFk, int kundeFk, LocalDate versicherungsbeginn) {
            this.produktFk = produktFk;
            this.kundeFk = kundeFk;
            this.versicherungsbeginn = versicherungsbeginn;
        }
    }

    // Struktur für Regelkomponenten
    private static class RegelKomponente {
        final String operator;
        final String wert; // Wert aus der Regel, noch als String

        RegelKomponente(String operator, String wert) {
            this.operator = operator;
            this.wert = wert;
        }
    }

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

    // --- Hilfsmethoden für Datenbankzugriffe ---

    private Vertragsdaten getVertragsdaten(Integer vertragsId) throws SQLException, VertragExistiertNichtException {
        String sql = "SELECT Produkt_FK, Kunde_FK, Versicherungsbeginn FROM Vertrag WHERE ID = ?";
        Connection conn = useConnection();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, vertragsId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return new Vertragsdaten(
                            rs.getInt("Produkt_FK"),
                            rs.getInt("Kunde_FK"),
                            rs.getDate("Versicherungsbeginn").toLocalDate()
                    );
                } else {
                    throw new VertragExistiertNichtException(vertragsId);
                }
            }
        }
    }

    private int getProduktFkForDeckungsart(Integer deckungsartId) throws SQLException, DeckungsartExistiertNichtException {
        String sql = "SELECT Produkt_FK FROM Deckungsart WHERE ID = ?";
        Connection conn = useConnection();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, deckungsartId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("Produkt_FK");
                } else {
                    throw new DeckungsartExistiertNichtException(deckungsartId);
                }
            }
        }
    }

    private Integer getDeckungsbetragDefinitionId(Integer deckungsartId, BigDecimal deckungsbetragValue) throws SQLException, UngueltigerDeckungsbetragException {
        // Prüft, ob dieser spezifische Deckungsbetragswert für die Deckungsart definiert ist
        // und gibt dessen ID aus der Tabelle Deckungsbetrag zurück.
        String sqlCheckBetragValue = "SELECT ID FROM Deckungsbetrag WHERE Deckungsart_FK = ? AND Deckungsbetrag = ?";
        Connection conn = useConnection();
        try (PreparedStatement pstmt = conn.prepareStatement(sqlCheckBetragValue)) {
            pstmt.setInt(1, deckungsartId);
            pstmt.setBigDecimal(2, deckungsbetragValue);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("ID");
                }
            }
        }

        // Wenn der spezifische Wert nicht gefunden wurde, prüfen wir, ob überhaupt Beträge für die Deckungsart existieren
        String sqlCheckAnyBetrag = "SELECT COUNT(*) FROM Deckungsbetrag WHERE Deckungsart_FK = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sqlCheckAnyBetrag)) {
            pstmt.setInt(1, deckungsartId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next() && rs.getInt(1) == 0) {
                    // Kein einziger Deckungsbetrag für diese Deckungsart definiert
                    throw new UngueltigerDeckungsbetragException(deckungsbetragValue); // Test createDeckung04
                }
            }
        }
        // Wenn wir hier sind, gibt es zwar Beträge für die Deckungsart, aber nicht den angefragten Wert
        throw new UngueltigerDeckungsbetragException(deckungsbetragValue);
    }


    private boolean isDeckungspreisVorhanden(Integer deckungsbetragDefinitionId, LocalDate relevantDate) throws SQLException {
        String sql = "SELECT COUNT(*) FROM Deckungspreis WHERE Deckungsbetrag_FK = ? AND ? >= Gueltig_Von AND ? <= Gueltig_Bis";
        Connection conn = useConnection();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, deckungsbetragDefinitionId);
            pstmt.setDate(2, Date.valueOf(relevantDate));
            pstmt.setDate(3, Date.valueOf(relevantDate));
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    private LocalDate getKundeGeburtsdatum(Integer kundeFk) throws SQLException, KundeExistiertNichtException {
        String sql = "SELECT Geburtsdatum FROM Kunde WHERE ID = ?";
        Connection conn = useConnection();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, kundeFk);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getDate("Geburtsdatum").toLocalDate();
                } else {
                    // Dieser Fall sollte eigentlich durch vorherige Prüfungen in createDeckung abgedeckt sein,
                    // aber zur Sicherheit hier eine Exception werfen.
                    throw new KundeExistiertNichtException(kundeFk);
                }
            }
        }
    }

    private List<Ablehnungsregel> getAblehnungsregeln(Integer deckungsartId) throws SQLException {
        List<Ablehnungsregel> regeln = new ArrayList<>();
        String sql = "SELECT R_Betrag, R_Alter FROM Ablehnungsregel WHERE Deckungsart_FK = ?";
        Connection conn = useConnection();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, deckungsartId);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    regeln.add(new Ablehnungsregel(rs.getString("R_Betrag"), rs.getString("R_Alter")));
                }
            }
        }
        return regeln;
    }
    // Interne Repräsentation einer Regel für die Verarbeitung
    private static class Ablehnungsregel {
        final String rBetragString;
        final String rAlterString;
        Ablehnungsregel(String rBetrag, String rAlter) {
            this.rBetragString = rBetrag;
            this.rAlterString = rAlter;
        }
    }


    // --- Hilfsmethoden für Regelauswertung (gemäß ablehnungsregeln.pdf) ---

    private int calculateAge(LocalDate birthDate, LocalDate referenceDate) {
        if ((birthDate != null) && (referenceDate != null)) {
            return Period.between(birthDate, referenceDate).getYears();
        } else {
            return 0; // Oder Fehler werfen, sollte nicht passieren bei validen Daten
        }
    }

    private RegelKomponente parseRegelString(String regelTeilString) {
        if (regelTeilString == null || regelTeilString.trim().isEmpty() || regelTeilString.trim().equals("-")) {
            // Behandelt null, leere Strings, nur Whitespace, oder einen einzelnen Bindestrich als "keine Regel"
            return new RegelKomponente("-", null);
        }

        // Spezifische Behandlung für Testdaten-Artefakt "- -"
        if (regelTeilString.trim().equals("- -")) {
            L.warn("Regel-Teil-String '{}' wird als 'keine Regel' interpretiert due to specific handling for '- -'.", regelTeilString);
            return new RegelKomponente("-", null);
        }

        // Pattern um Operator (>=, <=, !=, >, <, =) und Wert zu trennen
        // Geht von Operatoren am Anfang aus, gefolgt von der Zahl.
        // Erweitert um optionale Leerzeichen zwischen Operator und Zahl.
        Pattern pattern = Pattern.compile("^([><!]=?|=)\\s*(.*)");
        Matcher matcher = pattern.matcher(regelTeilString.trim());

        if (matcher.find()) {
            String operator = matcher.group(1);
            String wert = matcher.group(2).trim();
            if (wert.isEmpty()) { // Fall abfangen: Operator vorhanden, aber kein Wert (z.B. "> ")
                L.warn("Regel-Teil-String '{}' hat einen Operator aber keinen Wert, wird als 'keine Regel' interpretiert.", regelTeilString);
                return new RegelKomponente("-", null); // Oder spezifische Fehlerbehandlung
            }
            return new RegelKomponente(operator, wert);
        }

        L.error("Konnte Regel-Teil-String '{}' nicht mit Regex parsen. Wird als ungültiges Format behandelt.", regelTeilString);
        // Wenn es kein "-" ist und nicht geparst werden kann, ist es ein Datenproblem in Ablehnungsregel.
        throw new DataException("Ungültiges Format für Regelbestandteil: " + regelTeilString);
    }


    private boolean pruefeRegelbestandteil(BigDecimal aktuellerWert, String operator, String regelWertString) {
        if (operator.equals("-")) {
            return true;
        }
        if (aktuellerWert == null || regelWertString == null) {
            L.warn("Ungültige Eingabe für pruefeRegelbestandteil: aktuellerWert oder regelWertString ist null bei Operator {}", operator);
            return false; // Oder eine Exception werfen, da dies auf einen Fehler hindeutet
        }

        BigDecimal regelWertBigDecimal;
        try {
            regelWertBigDecimal = new BigDecimal(regelWertString);
        } catch (NumberFormatException e) {
            L.error("Fehler beim Konvertieren des Regelwerts '{}' zu BigDecimal.", regelWertString, e);
            throw new DataException("Ungültiger numerischer Wert im Regelbestandteil: " + regelWertString, e);
        }

        int vergleich = aktuellerWert.compareTo(regelWertBigDecimal);

        switch (operator) {
            case "=":  return vergleich == 0;
            case "!=": return vergleich != 0;
            case "<":  return vergleich <  0;
            case "<=": return vergleich <= 0;
            case ">":  return vergleich >  0;
            case ">=": return vergleich >= 0;
            default:
                L.error("Unbekannter Operator in Regel: {}", operator);
                throw new IllegalArgumentException("Unbekannter Operator: " + operator);
        }
    }

    // --- Hauptmethode createDeckung ---
    @Override
    public void createDeckung(Integer vertragsId, Integer deckungsartId, BigDecimal deckungsbetragValue) {
        L.info("createDeckung start: vertragsId={}, deckungsartId={}, deckungsbetragValue={}",
                vertragsId, deckungsartId, deckungsbetragValue);
        Connection conn = useConnection(); // Verbindung für die gesamte Operation holen

        try {
            // 1. Vertrag prüfen und Daten holen
            Vertragsdaten vertrag = getVertragsdaten(vertragsId); // Wirft VertragExistiertNichtException

            // 2. Deckungsart prüfen und deren Produkt_FK holen
            int daProduktFk = getProduktFkForDeckungsart(deckungsartId); // Wirft DeckungsartExistiertNichtException

            // 3. Prüfen, ob Deckungsart zum Produkt des Vertrags passt
            if (daProduktFk != vertrag.produktFk) {
                throw new DeckungsartPasstNichtZuProduktException(daProduktFk, vertrag.produktFk);
            }

            // 4. Prüfen, ob der Deckungsbetragswert für die Deckungsart gültig ist (definiert in Tabelle Deckungsbetrag)
            // Diese Methode wirft UngueltigerDeckungsbetragException, wenn nicht gültig, und gibt die ID des Deckungsbetrag-Eintrags zurück.
            Integer gewaehlterDeckungsbetragDefinitionId = getDeckungsbetragDefinitionId(deckungsartId, deckungsbetragValue);

            // 5. Prüfen, ob ein gültiger Deckungspreis für diesen Deckungsbetrag (definiert durch ID) zum Versicherungsbeginn existiert
            if (!isDeckungspreisVorhanden(gewaehlterDeckungsbetragDefinitionId, vertrag.versicherungsbeginn)) {
                throw new DeckungspreisNichtVorhandenException(deckungsbetragValue); // Die Ex. nimmt BigDecimal
            }

            // 6. Ablehnungsregeln prüfen
            LocalDate geburtsdatum = getKundeGeburtsdatum(vertrag.kundeFk); // KundeExistiertNichtException sollte hier nicht auftreten, da Vertrag.kundeFk gültig sein muss
            int alter = calculateAge(geburtsdatum, vertrag.versicherungsbeginn);
            List<Ablehnungsregel> regeln = getAblehnungsregeln(deckungsartId);

            for (Ablehnungsregel regel : regeln) {
                RegelKomponente betragRegelTeil = parseRegelString(regel.rBetragString);
                RegelKomponente alterRegelTeil = parseRegelString(regel.rAlterString);

                boolean betragBedingungErfuellt = pruefeRegelbestandteil(deckungsbetragValue, betragRegelTeil.operator, betragRegelTeil.wert);
                boolean alterBedingungErfuellt = pruefeRegelbestandteil(new BigDecimal(alter), alterRegelTeil.operator, alterRegelTeil.wert);

                if (betragBedingungErfuellt && alterBedingungErfuellt) {
                    L.warn("Ablehnungsregel getroffen für vertragsId={}, deckungsartId={}, betrag={}, alter={}. Regel: Betrag='{}', Alter='{}'",
                            vertragsId, deckungsartId, deckungsbetragValue, alter, regel.rBetragString, regel.rAlterString);
                    throw new DeckungsartNichtRegelkonformException(deckungsartId);
                }
            }

            // 7. Wenn alle Prüfungen erfolgreich: Deckung in Datenbank einfügen
            String insertSql = "INSERT INTO Deckung (Vertrag_FK, Deckungsart_FK, Deckungsbetrag) VALUES (?, ?, ?)";
            try (PreparedStatement pstmtInsert = conn.prepareStatement(insertSql)) {
                pstmtInsert.setInt(1, vertragsId);
                pstmtInsert.setInt(2, deckungsartId);
                pstmtInsert.setBigDecimal(3, deckungsbetragValue);

                int affectedRows = pstmtInsert.executeUpdate();
                if (affectedRows == 0) {
                    L.error("Einfügen der Deckung fehlgeschlagen, keine Zeile betroffen für vertragsId={}, deckungsartId={}", vertragsId, deckungsartId);
                    throw new DataException("Deckung konnte nicht eingefügt werden.");
                }
                L.info("Deckung erfolgreich eingefügt für vertragsId={}, deckungsartId={}", vertragsId, deckungsartId);
            }

        } catch (VertragExistiertNichtException | DeckungsartExistiertNichtException | UngueltigerDeckungsbetragException |
                 DeckungsartPasstNichtZuProduktException | DeckungspreisNichtVorhandenException | DeckungsartNichtRegelkonformException |
                 KundeExistiertNichtException e) { // KundeExistiertNichtException hier fangen, falls getKundeGeburtsdatum sie doch wirft
            L.warn("Fehler beim Erstellen der Deckung (fachliche Exception): {}", e.getMessage());
            throw e; // Fachliche Exceptions direkt weiterwerfen
        } catch (SQLException e) {
            L.error("SQL-Fehler beim Erstellen der Deckung für vertragsId=" + vertragsId, e);
            throw new DataException("Datenbankfehler beim Erstellen der Deckung: " + e.getMessage(), e);
        } catch (IllegalArgumentException e) { // Für ungültige Operatoren in Regeln
            L.error("Fehler in Regeldefinition: " + e.getMessage(), e);
            throw new DataException("Fehler in Regeldefinition: " + e.getMessage(), e);
        }
        L.info("createDeckung ende: vertragsId={}, deckungsartId={}", vertragsId, deckungsartId);
    }
}