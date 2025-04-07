package com.mycompany.eventmanagement.dao;

import com.mycompany.eventmanagement.model.RSVP;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Data Access Object for RSVP entities
 * Handles database operations related to event RSVPs
 */
public class RSVPDAO {
    
    /**
     * Creates a new RSVP for an event
     * 
     * @param rsvp RSVP object to save
     * @return ID of the created RSVP, or -1 if operation failed
     */
    public int createRSVP(RSVP rsvp) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        int newRsvpId = -1;
        
        try {
            conn = DatabaseConnection.getConnection();
            
            // Check if the user already has an RSVP for this event
            String checkSql = "SELECT rsvp_id FROM rsvps WHERE user_email = ? AND event_id = ?";
            PreparedStatement checkStmt = conn.prepareStatement(checkSql);
            checkStmt.setString(1, rsvp.getUserEmail());
            checkStmt.setInt(2, rsvp.getEventId());
            ResultSet checkRs = checkStmt.executeQuery();
            
            if (checkRs.next()) {
                // User already has an RSVP, update it instead
                int existingRsvpId = checkRs.getInt("rsvp_id");
                checkRs.close();
                checkStmt.close();
                
                String updateSql = "UPDATE rsvps SET attendee_count = ? WHERE rsvp_id = ?";
                PreparedStatement updateStmt = conn.prepareStatement(updateSql);
                updateStmt.setInt(1, rsvp.getAttendeeCount());
                updateStmt.setInt(2, existingRsvpId);
                updateStmt.executeUpdate();
                updateStmt.close();
                
                return existingRsvpId;
            }
            
            checkRs.close();
            checkStmt.close();
            
            // Create new RSVP
            String sql = "INSERT INTO rsvps (event_id, user_name, user_email, attendee_count) VALUES (?, ?, ?, ?)";
            stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            
            stmt.setInt(1, rsvp.getEventId());
            stmt.setString(2, rsvp.getUserName());
            stmt.setString(3, rsvp.getUserEmail());
            stmt.setInt(4, rsvp.getAttendeeCount());
            
            int affectedRows = stmt.executeUpdate();
            
            if (affectedRows > 0) {
                rs = stmt.getGeneratedKeys();
                if (rs.next()) {
                    newRsvpId = rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            System.err.println("Error creating RSVP: " + e.getMessage());
        } finally {
            closeResources(conn, stmt, rs);
        }
        
        return newRsvpId;
    }
    
    /**
     * Deletes an RSVP from the database
     * 
     * @param rsvpId ID of the RSVP to delete
     * @return true if deletion was successful, false otherwise
     */
    public boolean deleteRSVP(int rsvpId) {
        Connection conn = null;
        PreparedStatement stmt = null;
        boolean success = false;
        
        try {
            conn = DatabaseConnection.getConnection();
            String sql = "DELETE FROM rsvps WHERE rsvp_id = ?";
            stmt = conn.prepareStatement(sql);
            
            stmt.setInt(1, rsvpId);
            
            int affectedRows = stmt.executeUpdate();
            success = (affectedRows > 0);
        } catch (SQLException e) {
            System.err.println("Error deleting RSVP: " + e.getMessage());
        } finally {
            closeResources(conn, stmt, null);
        }
        
        return success;
    }
    
    /**
     * Retrieves all RSVPs for a specific event
     * 
     * @param eventId ID of the event
     * @return List of RSVPs for the event
     */
    public List<RSVP> getRSVPsByEvent(int eventId) {
        List<RSVP> rsvps = new ArrayList<>();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            conn = DatabaseConnection.getConnection();
            String sql = "SELECT * FROM rsvps WHERE event_id = ?";
            stmt = conn.prepareStatement(sql);
            
            stmt.setInt(1, eventId);
            
            rs = stmt.executeQuery();
            
            while (rs.next()) {
                RSVP rsvp = new RSVP();
                rsvp.setRsvpId(rs.getInt("rsvp_id"));
                rsvp.setEventId(rs.getInt("event_id"));
                rsvp.setUserName(rs.getString("user_name"));
                rsvp.setUserEmail(rs.getString("user_email"));
                rsvp.setAttendeeCount(rs.getInt("attendee_count"));
                rsvps.add(rsvp);
            }
        } catch (SQLException e) {
            System.err.println("Error retrieving RSVPs: " + e.getMessage());
        } finally {
            closeResources(conn, stmt, rs);
        }
        
        return rsvps;
    }
    
    /**
     * Counts the total number of attendees for an event
     * 
     * @param eventId ID of the event
     * @return Total number of attendees
     */
    public int getTotalAttendees(int eventId) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        int totalAttendees = 0;
        
        try {
            conn = DatabaseConnection.getConnection();
            String sql = "SELECT SUM(attendee_count) as total FROM rsvps WHERE event_id = ?";
            stmt = conn.prepareStatement(sql);
            
            stmt.setInt(1, eventId);
            
            rs = stmt.executeQuery();
            
            if (rs.next()) {
                totalAttendees = rs.getInt("total");
            }
        } catch (SQLException e) {
            System.err.println("Error counting attendees: " + e.getMessage());
        } finally {
            closeResources(conn, stmt, rs);
        }
        
        return totalAttendees;
    }
    
    /**
     * Checks if a user has already RSVP'd for an event
     * 
     * @param eventId ID of the event
     * @param userEmail Email of the user
     * @return RSVP object if found, null otherwise
     */
    public RSVP getUserRSVP(int eventId, String userEmail) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        RSVP rsvp = null;
        
        try {
            conn = DatabaseConnection.getConnection();
            String sql = "SELECT * FROM rsvps WHERE event_id = ? AND user_email = ?";
            stmt = conn.prepareStatement(sql);
            
            stmt.setInt(1, eventId);
            stmt.setString(2, userEmail);
            
            rs = stmt.executeQuery();
            
            if (rs.next()) {
                rsvp = new RSVP();
                rsvp.setRsvpId(rs.getInt("rsvp_id"));
                rsvp.setEventId(rs.getInt("event_id"));
                rsvp.setUserName(rs.getString("user_name"));
                rsvp.setUserEmail(rs.getString("user_email"));
                rsvp.setAttendeeCount(rs.getInt("attendee_count"));
            }
        } catch (SQLException e) {
            System.err.println("Error checking user RSVP: " + e.getMessage());
        } finally {
            closeResources(conn, stmt, rs);
        }
        
        return rsvp;
    }
    
    /**
     * Helper method to close database resources
     */
    private void closeResources(Connection conn, PreparedStatement stmt, ResultSet rs) {
        try {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
            if (conn != null) DatabaseConnection.closeConnection(conn);
        } catch (SQLException e) {
            System.err.println("Error closing database resources: " + e.getMessage());
        }
    }
}
