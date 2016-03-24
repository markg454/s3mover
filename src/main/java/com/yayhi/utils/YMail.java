package com.yayhi.utils;

import java.io.File;
import java.util.Date;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

public class YMail {
	
	public YMail() {
		
	}
	
	public void sendMail(String mailServer, String from, String to, String subject, String messageBody) throws MessagingException, AddressException {
		
    	// Setup mail server
		Properties props = System.getProperties();
		props.put("mail.smtp.host", "localhost");
		
		// Create an array of email addresses
		String[] emailAddresses;
		String delimiter = ",";

		// create an array of email address
		emailAddresses = to.split(delimiter);
		
		// Get a mail session
		Session session = Session.getDefaultInstance(props, null);
		
		// Define a new mail message
		Message message = new MimeMessage(session);
		message.setFrom(new InternetAddress(from));
		
		// Send to multiple recipients
		for(int i =0; i < emailAddresses.length ; i++) {
			
			message.addRecipient(Message.RecipientType.TO, new InternetAddress(emailAddresses[i]));
			message.setSubject(subject);
			
		}
		
		message.setSentDate(new Date());
		//message.setContent(messageBody, "text/plain");
		
		// create the message part 
	    MimeBodyPart messageBodyPart = new MimeBodyPart();

	    // message body
	    messageBodyPart.setText(messageBody);

	    Multipart multipart = new MimeMultipart();
	    multipart.addBodyPart(messageBodyPart);

	    // Put parts in message
	    message.setContent(multipart);
	
		// Send the message
		Transport.send(message);
		
	} 

	public void sendMail(String mailServer, String from, String to, String subject, String messageBody, File fileAttachment) throws MessagingException, AddressException {
	
		// Setup mail server
		Properties props = System.getProperties();
		props.put("mail.smtp.host", "localhost");
		
		// Create an array of email addresses
		String[] emailAddresses;
		String delimiter = ",";
	
		// create an array of email address
		emailAddresses = to.split(delimiter);
		
		// Get a mail session
		Session session = Session.getDefaultInstance(props, null);
		
		// Define a new mail message
		Message message = new MimeMessage(session);
		message.setFrom(new InternetAddress(from));
		
		// Send to multiple recipients
		for(int i =0; i < emailAddresses.length ; i++) {
			
			message.addRecipient(Message.RecipientType.TO, new InternetAddress(emailAddresses[i]));
			message.setSubject(subject);
			
		}
		
		message.setSentDate(new Date());
		//message.setContent(messageBody, "text/plain");
		
		// create the message part 
	    MimeBodyPart messageBodyPart = new MimeBodyPart();
	
	    // message body
	    messageBodyPart.setText(messageBody);
	
	    Multipart multipart = new MimeMultipart();
	    multipart.addBodyPart(messageBodyPart);
	    
	    // attachment
	    if (fileAttachment != null) {
	    	
		    messageBodyPart = new MimeBodyPart();
		    DataSource source = new FileDataSource(fileAttachment);
		    messageBodyPart.setDataHandler(new DataHandler(source));
		    messageBodyPart.setFileName(fileAttachment.getName());
		    multipart.addBodyPart(messageBodyPart);
		    
	    }
	
	    // Put parts in message
	    message.setContent(multipart);
	
		// Send the message
		Transport.send(message);
		
	} 

}
