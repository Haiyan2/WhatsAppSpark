package com.yan2.spark;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * WhatsAppMessage JavaBean.
 * 
 * @author Haiyan
 *
 */
public class WhatsAppMessage implements Serializable {
    /**
     * Serial ID.
     */
    private static final long serialVersionUID = -2709673546397527788L;
    private Timestamp dateTime;
    private String user;
    private String message;
    private int lineCount = 1;

    public Timestamp getDateTime() {
        return dateTime;
    }

    public void setDateTime(Timestamp dateTime) {
        this.dateTime = dateTime;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getLineCount() {
        return lineCount;
    }

    public void setLineCount(int lineCount) {
        this.lineCount = lineCount;
    }

    @Override
    public String toString() {
        return "WhatsAppMessage [dateTime=" + dateTime + ", user=" + user + ", message=" + message + ", lineCount="
                + lineCount + "]";
    }
}
