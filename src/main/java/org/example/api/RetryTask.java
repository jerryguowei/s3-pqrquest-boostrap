package org.example.api;

import org.example.exceptions.RequireRetryException;
import org.example.exceptions.RetryFailedException;

public interface RetryTask<T> {
    T run() throws RequireRetryException;
    
	static <T> T runWithRetries(int maxRetries, RetryTask<T> t) { 
	    int count = 0;
	    while (count < maxRetries) {
	        try {
	            return t.run();
	         }
	        catch (RequireRetryException e) {
	        	System.out.println("retry..");
	            if (++count >= maxRetries) break;
	        }
	    }
		throw new RetryFailedException();
	}
}