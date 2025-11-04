package com.iksanov.distributedcache.node.consensus.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;

/**
 * Postgres-backed RaftPersistence.
 * Table DDL:
 * CREATE TABLE IF NOT EXISTS raft_state (
 *   node_id TEXT PRIMARY KEY,
 *   current_term BIGINT NOT NULL DEFAULT 0,
 *   voted_for TEXT
 * );
 */
public class PostgresRaftPersistence implements RaftPersistence {
    private static final Logger log = LoggerFactory.getLogger(PostgresRaftPersistence.class);

    private final DataSource ds;
    private final String nodeId;

    public PostgresRaftPersistence(DataSource ds, String nodeId) {
        this.ds = ds;
        this.nodeId = nodeId;
        ensureRowExists();
    }

    private void ensureRowExists() {
        final String sql = "INSERT INTO raft_state(node_id, current_term, voted_for) VALUES (?, 0, NULL) ON CONFLICT (node_id) DO NOTHING";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, nodeId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to ensure raft_state row exists for " + nodeId, e);
        }
    }

    @Override
    public synchronized void saveTerm(long term) throws IOException {
        final String sql = "INSERT INTO raft_state(node_id, current_term) VALUES (?, ?) " +
                           "ON CONFLICT (node_id) DO UPDATE SET current_term = EXCLUDED.current_term";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, nodeId);
            ps.setLong(2, term);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to save term to Postgres", e);
        }
    }

    @Override
    public synchronized long loadTerm() throws IOException {
        final String sql = "SELECT current_term FROM raft_state WHERE node_id = ?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, nodeId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getLong(1);
                return 0L;
            }
        } catch (SQLException e) {
            throw new IOException("Failed to load term from Postgres", e);
        }
    }

    @Override
    public synchronized void saveVotedFor(String votedFor) throws IOException {
        final String sql = "INSERT INTO raft_state(node_id, voted_for) VALUES (?, ?) " +
                           "ON CONFLICT (node_id) DO UPDATE SET voted_for = EXCLUDED.voted_for";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, nodeId);
            if (votedFor == null) ps.setNull(2, Types.VARCHAR);
            else ps.setString(2, votedFor);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to save votedFor to Postgres", e);
        }
    }

    @Override
    public synchronized String loadVotedFor() throws IOException {
        final String sql = "SELECT voted_for FROM raft_state WHERE node_id = ?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, nodeId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String v = rs.getString(1);
                    return (v == null || v.isEmpty()) ? null : v;
                }
                return null;
            }
        } catch (SQLException e) {
            throw new IOException("Failed to load votedFor from Postgres", e);
        }
    }
}
