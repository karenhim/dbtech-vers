package de.htwberlin.dbtech.aufgaben.ue03;

/*
  @author Ingo Classen
 */

import de.htwberlin.dbtech.exceptions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * VersicherungJdbc
 */
public class VersicherungService implements IVersicherungService {
    private static final Logger L = LoggerFactory.getLogger(VersicherungService.class);
    private Connection connection;

    @Override
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    @SuppressWarnings("unused")
    private Connection useConnection() {
        if (connection == null) {
            throw new DataException("Connection not set");
        }
        return connection;
    }

    @Override
    public void createDeckung(Integer vertragsId, Integer deckungsartId, BigDecimal deckungsbetrag) {
        L.info("vertragsId: " + vertragsId);
        L.info("deckungsartId: " + deckungsartId);
        L.info("deckungsbetrag: " + deckungsbetrag);
        L.info("ende");

        try {
            if (!vertragExistiert(vertragsId)) {
                throw new VertragExistiertNichtException(vertragsId);
            }
            if (!DeckungsartExistiert(deckungsartId)) {
                throw new DeckungsartExistiertNichtException(deckungsartId);
            }
            if (!DeckungsartPasstZuProdukt(vertragsId, deckungsartId)) {
                throw new DeckungsartPasstNichtZuProduktException(vertragsId, deckungsartId);
            }
            if (!GueltigerDeckungsbetrag(deckungsartId, deckungsbetrag)) {
                throw new UngueltigerDeckungsbetragException(deckungsartId, deckungsbetrag);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Make sure vertrag exists according to given ID
     *
     * @param vertragsId
     * @return
     * @throws SQLException
     */
    private boolean vertragExistiert(Integer vertragsId) throws SQLException {
        String sql = "SELECT COUNT(*) FROM Vertrag WHERE ID=?";

        try (PreparedStatement stmt = useConnection().prepareStatement(sql)) {
            stmt.setInt(1, vertragsId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int count = rs.getInt(1);
                    return count > 0;
                }
                return false;
            }
        } catch (SQLException e) {
            L.error("Database error while checking Vertrag ID {}", vertragsId, e);
            throw new DataException("Database error while checking Vertrag ID {}", e);
        }
    }

    /**
     * Make sure the coverage type is defined in the Deckungsart table
     *
     * @param deckungsartId
     * @return
     * @throws SQLException
     */
    private boolean DeckungsartExistiert(Integer deckungsartId) throws SQLException {
        String sql = "SELECT COUNT(*) FROM Deckungsart WHERE ID=?";
        try (PreparedStatement stmt = useConnection().prepareStatement(sql)) {
            stmt.setInt(1, deckungsartId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int count = rs.getInt(1);
                    return count > 0;
                }
                return false;
            }

        } catch (SQLException e) {
            L.error("Database error while checking Deckungsart ID {}", deckungsartId, e);
            throw new DataException("Database error while checking Deckungsart ID {}", e);
        }
    }

    private boolean DeckungsartPasstZuProdukt(Integer vertragsId, Integer deckungsartId) throws SQLException {
        String sql = "SELECT COUNT(*) " +
                "FROM Vertrag v " +
                "JOIN Deckungsart da ON v.Produkt_FK = da.Produkt_FK " +
                "WHERE v.ID=? AND da.ID=? ";

        try (PreparedStatement stmt = useConnection().prepareStatement(sql)) {
            stmt.setInt(1, vertragsId);
            stmt.setInt(2, deckungsartId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int count = rs.getInt(1);
                    return count > 0;
                }
                return false;
            }

        } catch (SQLException e) {
            L.error("Database error while checking Product-Deckungsart relationship", e);
            throw new DataException("Database error while checking Product-Deckungsart relationship", e);
        }
    }

    private boolean GueltigerDeckungsbetrag(Integer deckungsartId, BigDecimal deckungsbetrag)  {
        String sql= "SELECT COUNT(*) " +
                "FROM Deckungsbetrag "+
                "WHERE Deckungsart_FK=? "+
                "AND Deckungsbetrag=?";

        try (PreparedStatement stmt = useConnection().prepareStatement(sql)) {
            stmt.setInt(1, deckungsartId);
            stmt.setBigDecimal(2, deckungsbetrag);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int count = rs.getInt(1);
                    return count > 0;
                }
                return false;
            }

        } catch (SQLException e) {
            L.error("Database error while checking Deckungsbetrag", e);
            throw new DataException("Database error while checking Deckungsbetrag", e);
        }
    }

}
