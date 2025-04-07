package com.mycompany.eventmanagement.dao;

import com.mycompany.eventmanagement.model.Event;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Data Access Object for Event entities
 * Handles database operations related to events
 */
public class EventDAO {
    
    /**
     * Retrieves all events from the database
     * 
     * @return List of all events
     */
    public List<Event> getAllEvents() {
        List<Event> events = new ArrayList<>();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            conn = DatabaseConnection.getConnection();
            String sql = "SELECT * FROM events ORDER BY event_date DESC";
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            
            while (rs.next()) {
                events.add(extractEventFromResultSet(rs));
            }
        } catch (SQLException e) {
            System.err.println("Error retrieving events: " + e.getMessage());
        } finally {
            closeResources(conn, stmt, rs);
        }
        
        return events;
    }
    
    /**
     * Retrieves events filtered by event type
     * 
     * @param eventType The type of events to retrieve
     * @return List of filtered events
     */
    public List<Event> getEventsByType(String eventType) {
        List<Event> events = new ArrayList<>();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            conn = DatabaseConnection.getConnection();
            String sql = "SELECT * FROM events WHERE event_type = ? ORDER BY event_date DESC";
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, eventType);
            rs = stmt.executeQuery();
            
            while (rs.next()) {
                events.add(extractEventFromResultSet(rs));
            }
        } catch (SQLException e) {
            System.err.println("Error retrieving events by type: " + e.getMessage());
        } finally {
            closeResources(conn, stmt, rs);
        }
        
        return events;
    }
    
    /**
     * Searches events by various criteria
     * 
     * @param type Event type to search for (can be null)
     * @param location Location to search for (can be null)
     * @param date Date to search for (can be null)
     * @return List of matching events
     */
    public List<Event> searchEvents(String type, String location, Date date) {
        List<Event> events = new ArrayList<>();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            conn = DatabaseConnection.getConnection();
            StringBuilder sqlBuilder = new StringBuilder("SELECT * FROM events WHERE 1=1");
            List<Object> params = new ArrayList<>();
            
            if (type != null && !type.isEmpty()) {
                sqlBuilder.append(" AND event_type = ?");
                params.add(type);
            }
            
            if (location != null && !location.isEmpty()) {
                sqlBuilder.append(" AND location LIKE ?");
                params.add("%" + location + "%");
            }
            
            if (date != null) {
                sqlBuilder.append(" AND DATE(event_date) = DATE(?)");
                params.add(new Timestamp(date.getTime()));
            }
            
            sqlBuilder.append(" ORDER BY event_date DESC");
            
            stmt = conn.prepareStatement(sqlBuilder.toString());
            
            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }
            
            rs = stmt.executeQuery();
            
            while (rs.next()) {
                events.add(extractEventFromResultSet(rs));
            }
        } catch (SQLException e) {
            System.err.println("Error searching events: " + e.getMessage());
        } finally {
            closeResources(conn, stmt, rs);
        }
        
        return events;
    }
    
    /**
     * Retrieves a single event by its ID
     * 
     * @param eventId ID of the event
     * @return Event object or null if not found
     */
    public Event getEventById(int eventId) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        Event event = null;
        
        try {
            conn = DatabaseConnection.getConnection();
            String sql = "SELECT * FROM events WHERE event_id = ?";
            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, eventId);
            rs = stmt.executeQuery();
            
            if (rs.next()) {
                event = extractEventFromResultSet(rs);
            }
        } catch (SQLException e) {
            System.err.println("Error retrieving event by ID: " + e.getMessage());
        } finally {
            closeResources(conn, stmt, rs);
        }
        
        return event;
    }
    
    /**
     * Adds a new event to the database
     * 
     * @param event Event to add
     * @return ID of the newly created event, or -1 if operation failed
     */
    public int addEvent(Event event) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        int newEventId = -1;
        
        try {
            conn = DatabaseConnection.getConnection();
            String sql = "INSERT INTO events (event_name, event_date, location, description, event_type, max_attendees, created_by) " +
                         "VALUES (?, ?, ?, ?, ?, ?, ?)";
            stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            
            stmt.setString(1, event.getEventName());
            stmt.setTimestamp(2, new Timestamp(event.getEventDate().getTime()));
            stmt.setString(3, event.getLocation());
            stmt.setString(4, event.getDescription());
            stmt.setString(5, event.getEventType());
            stmt.setInt(6, event.getMaxAttendees());
            stmt.setString(7, event.getCreatedBy());
            
            int affectedRows = stmt.executeUpdate();
            
            if (affectedRows > 0) {
                rs = stmt.getGeneratedKeys();
                if (rs.next()) {
                    newEventId = rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            System.err.println("Error adding event: " + e.getMessage());
        } finally {
            closeResources(conn, stmt, rs);
        }
        
        return newEventId;
    }
    
    /**
     * Updates an existing event
     * 
     * @param event Event with updated information
     * @return true if update was successful, false otherwise
     */
    public boolean updateEvent(Event event) {
        Connection conn = null;
        PreparedStatement stmt = null;
        boolean success = false;
        
        try {
            conn = DatabaseConnection.getConnection();
            String sql = "UPDATE events SET event_name = ?, event_date = ?, location = ?, " +
                         "description = ?, event_type = ?, max_attendees = ? " +
                         "WHERE event_id = ?";
            stmt = conn.prepareStatement(sql);
            
            stmt.setString(1, event.getEventName());
            stmt.setTimestamp(2, new Timestamp(event.getEventDate().getTime()));
            stmt.setString(3, event.getLocation());
            stmt.setString(4, event.getDescription());
            stmt.setString(5, event.getEventType());
            stmt.setInt(6, event.getMaxAttendees());
            stmt.setInt(7, event.getEventId());
            
            int affectedRows = stmt.executeUpdate();
            success = (affectedRows > 0);
        } catch (SQLException e) {
            System.err.println("Error updating event: " + e.getMessage());
        } finally {
            closeResources(conn, stmt, null);
        }
        
        return success;
    }
    
    /**
     * Deletes an event from the database
     * 
     * @param eventId ID of the event to delete
     * @return true if deletion was successful, false otherwise
     */
    public boolean deleteEvent(int eventId) {
        Connection conn = null;
        PreparedStatement stmt = null;
        boolean success = false;
        
        try {
            conn = DatabaseConnection.getConnection();
            
            // First delete any RSVPs for this event
            String deleteRSVPsSql = "DELETE FROM rsvps WHERE event_id = ?";
            PreparedStatement deleteRSVPsStmt = conn.prepareStatement(deleteRSVPsSql);
            deleteRSVPsStmt.setInt(1, eventId);
            deleteRSVPsStmt.executeUpdate();
            deleteRSVPsStmt.close();
            
            // Then delete the event
            String sql = "DELETE FROM events WHERE event_id = ?";
            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, eventId);
            
            int affectedRows = stmt.executeUpdate();
            success = (affectedRows > 0);
        } catch (SQLException e) {
            System.err.println("Error deleting event: " + e.getMessage());
        } finally {
            closeResources(conn, stmt, null);
        }
        
        return success;
    }
    
    /**
     * Updates the current attendees count for an event
     * 
     * @param eventId ID of the event
     * @param count New count of attendees
     * @return true if update was successful, false otherwise
     */
    public boolean updateAttendeeCount(int eventId, int count) {
        Connection conn = null;
        PreparedStatement stmt = null;
        boolean success = false;
        
        try {
            conn = DatabaseConnection.getConnection();
            String sql = "UPDATE events SET current_attendees = ? WHERE event_id = ?";
            stmt = conn.prepareStatement(sql);
            
            stmt.setInt(1, count);
            stmt.setInt(2, eventId);
            
            int affectedRows = stmt.executeUpdate();
            success = (affectedRows > 0);
        } catch (SQLException e) {
            System.err.println("Error updating attendee count: " + e.getMessage());
        } finally {
            closeResources(conn, stmt, null);
        }
        
        return success;
    }
    
    /**
     * Helper method to extract Event object from ResultSet
     */
    private Event extractEventFromResultSet(ResultSet rs) throws SQLException {
        Event event = new Event();
        event.setEventId(rs.getInt("event_id"));
        event.setEventName(rs.getString("event_name"));
        event.setEventDate(rs.getTimestamp("event_date"));
        event.setLocation(rs.getString("location"));
        event.setDescription(rs.getString("description"));
        event.setEventType(rs.getString("event_type"));
        event.setMaxAttendees(rs.getInt("max_attendees"));
        event.setCurrentAttendees(rs.getInt("current_attendees"));
        event.setCreatedBy(rs.getString("created_by"));
        return event;
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
