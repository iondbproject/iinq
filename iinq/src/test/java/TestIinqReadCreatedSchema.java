/******************************************************************************/
/**
 @file TestIinqReadCreatedSchema.java
 @author Kai Neubauer
 @brief        Tests that XML files with iinq can be read successfully.
 @copyright Copyright 2017
 The University of British Columbia,
 IonDB Project Contributors (see AUTHORS.md)
 @par Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions are met:

 @par 1.Redistributions of source code must retain the above copyright notice,
 this list of conditions and the following disclaimer.

 @par 2.Redistributions in binary form must reproduce the above copyright notice,
 this list of conditions and the following disclaimer in the documentation
 and/or other materials provided with the distribution.

 @par 3.Neither the name of the copyright holder nor the names of its contributors
 may be used to endorse or promote products derived from this software without
 specific prior written permission.

 @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 POSSIBILITY OF SUCH DAMAGE.
 */
/******************************************************************************/

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import unity.annotation.GlobalSchema;
import unity.annotation.SourceDatabase;
import unity.annotation.SourceField;
import unity.annotation.SourceTable;
import unity.jdbc.UnityConnection;
import unity.jdbc.UnityStatement;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

public class TestIinqReadCreatedSchema {
	@Test
	public void readIinqSources() {
		GlobalSchema metadata = null;
		Connection con = null;
		String url = "jdbc:unity://data/xspec/iinq_sources.xml";
		try {
			Class.forName("unity.jdbc.UnityDriver");
			System.out.println("\nGetting connection: " + url);
			con = DriverManager.getConnection(url);
			System.out.println("\nConnection successful for " + url);
			System.out.println("\nGetting metadata.");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != con) {
					con.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
