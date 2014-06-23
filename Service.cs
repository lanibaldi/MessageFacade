using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Web;
using System.Xml;
using System.Xml.Schema;
using System.Xml.Serialization;
using log4net;
using XmlValidation;

namespace Ors.Privacy.Audit.Facade
{
    public class Service : IHttpHandler
    {
        #region Private Members

        private ErrorHandler errHandler;

        #endregion

        #region Handler

        bool IHttpHandler.IsReusable
        {
            get { throw new NotImplementedException(); }
        }

        void IHttpHandler.ProcessRequest(HttpContext context)
        {
            ILog logger = LogManager.GetLogger(GetType().FullName);

            try
            {
                logger.Info(string.Format("Request HttpMethod={0}", context.Request.HttpMethod));

                string url = Convert.ToString(context.Request.Url);
                logger.Info(string.Format("Request Url={0}", url));

                errHandler = new ErrorHandler();

                //Handling CRUD
                switch (context.Request.HttpMethod)
                {
                    case "GET":
                        //Perform READ Operation                   
                        READ(context);
                        break;
                    case "POST":
                        //Perform CREATE Operation
                        CREATE(context);
                        break;
                    case "PUT":
                        //Perform UPDATE Operation
                        UPDATE(context);
                        break;
                    case "DELETE":
                        //Perform DELETE Operation
                        DELETE(context);
                        break;
                    default:
                        break;
                }
            }            
            catch (Exception exc)
            {
                logger.Error("Failed processing HTTP request.", exc);

                errHandler.ErrorMessage = exc.Message;
                context.Response.Write(errHandler.ErrorMessage);
            }
        }

        #endregion

        #region CRUD Functions
        /// <summary>
        /// CREATE function
        /// </summary>
        /// <param name="context"></param>
        private void CREATE(HttpContext context)
        {
            ILog logger = LogManager.GetLogger(GetType().FullName);

            try
            {
                // HTTP POST sends name/value pairs to a web server
                // data is sent in message body

                // This Post task handles cookies and remembers them across calls. 
                // This means that you can post to a login form, receive authentication cookies, 
                // then subsequent posts will automatically pass the correct cookies. 
                // The cookies are stored in memory only, they are not written to disk and 
                // will cease to exist upon completion of the build.

                // The message body is posted as bytes. 
                logger.Info(string.Format("Request ContentLength={0}", context.Request.ContentLength));
                byte[] postData = context.Request.BinaryRead(context.Request.ContentLength);
                if (postData == null)
                {
                    WriteResponse("FAIL");
                }
                else
                {
                    WriteResponse("OK");
                    //Convert the bytes to string using Encoding class
                    string xmlContent = Encoding.UTF8.GetString(postData);
                    logger.Info(string.Format("Request ContentText={0}", xmlContent));

                    //Validate content
                    if (ValidateXmlContent(logger, xmlContent))
                    {
                        logger.Info("Xml content successfully validated.");
                        //Send Xml content
                        if (SendXmlContent(logger, xmlContent))
                        {
                            logger.Info("Xml content successfully sent.");
                        }
                    }
                }
            }
            catch (HttpException hexc)
            {
                logger.Error("Failed processing POST request.", hexc);

                WriteResponse(string.Format("Error in CREATE: {0}", hexc.Message));
                errHandler.ErrorMessage = hexc.Message;
            }
            catch (Exception exc)
            {
                logger.Error("Failed processing POST request.", exc);

                WriteResponse(string.Format("Error in CREATE: {0}", exc.Message));
                errHandler.ErrorMessage = exc.Message;
            }
        }

        private static bool SendXmlContent(ILog logger, string xmlContent)
        {
            bool isSent = false;
            try
            {
                // Queue name
                string queueName = ConfigurationManager.AppSettings["QueueName"];
                if (string.IsNullOrEmpty(queueName))
                    queueName = @".\private$\my_queue";
                logger.Debug(string.Format("QueueName=\"{0}\"", queueName));
                // Send content to queue
                var msgBuilder = new QueueMessaging.MessageBuilder();
                var msgText = msgBuilder.BuildQueueMessage(xmlContent);

                var msgSender = new QueueMessaging.MessageSender();
                isSent = msgSender.SendQueueMessage(queueName, msgText);
            }
            catch (Exception exc)
            {
                logger.Error("Failed sending Xml content.", exc);

                throw new Exception("Failed sending Xml content.", exc);
            }
            return isSent;
        }

        private static bool ValidateXmlContent(ILog logger, string xmlContent)
        {
            bool isValidated = false;

            try
            {
                // Schema files path
                string schemaFiles = ConfigurationManager.AppSettings["SchemaFiles"];
                if (string.IsNullOrEmpty(schemaFiles))
                    schemaFiles = @"XmlValidation.Schemas.Common.xsd";
                logger.Debug(string.Format("SchemaFiles=\"{0}\"", schemaFiles));

                var currentSchemas = schemaFiles.Split(new char[] { '|' });                

                SchemaValidator validator = new SchemaValidator(currentSchemas, false);                
                validator.WarningAsErrors = true;
                validator.Validate(xmlContent);
                if (string.IsNullOrEmpty(validator.ErrorMessage))
                {
                    logger.Debug("Validator error message is empty.");
                    isValidated = true;
                }
                else
                {
                    logger.Warn("Validator error message is not empty.");
                    throw new Exception(validator.ErrorMessage);
                }

            }
            catch (XmlSchemaException xsexc)
            {
                logger.Error("Failed validating Xml content.", xsexc);
            }
            return isValidated;
        }        
        /// <summary>
        /// GET function
        /// </summary>
        /// <param name="context"></param>
        private void READ(HttpContext context)
        {
            ILog logger = LogManager.GetLogger(GetType().FullName);

            //HTTP Request - http://localhost/Ors.Privacy.Audit/audit 
            try
            {
                // Schema file
                string schemaFile = @"Ors.Privacy.Audit.XmlValidation.Schemas.Common.xsd";
                string schemaString = SchemaValidator.GetStringResource(schemaFile);

                context.Response.ContentType = "text/xml";
                WriteResponse(schemaString);
            }
            catch (HttpException hexc)
            {
                logger.Error("Failed processing READ request.", hexc);

                WriteResponse(string.Format("Error in READ: {0}", hexc.Message));
                errHandler.ErrorMessage = hexc.Message;
            }
            catch (Exception exc)
            {
                logger.Error("Failed processing READ request.", exc);

                WriteResponse(string.Format("Error in READ: {0}", exc.Message));
                errHandler.ErrorMessage = exc.Message;
            }
        }
        /// <summary>
        /// UPDATE function
        /// </summary>
        /// <param name="context"></param>
        private void UPDATE(HttpContext context)
        {
            ILog logger = LogManager.GetLogger(GetType().FullName);

            try
            {
                // Queue name
                string queueName = ConfigurationManager.AppSettings["QueueName"];
                if (string.IsNullOrEmpty(queueName))
                    queueName = @".\private$\my_queue";
                logger.Debug(string.Format("QueueName=\"{0}\"", queueName));

                var msgCounter = new QueueMessaging.MessageCounter();
                long count = msgCounter.CountQueueMessages(queueName);

                context.Response.ContentType = "text/xml";
                WriteResponse(string.Format("{0} - Queue \"{1}\" contains {2} messages.",
                    DateTime.Now.ToString("s"), queueName, count));
            }
            catch (HttpException hexc)
            {
                logger.Error("Failed processing UPDATE request.", hexc);

                WriteResponse(string.Format("Error in UPDATE: {0}", hexc.Message));
                errHandler.ErrorMessage = hexc.Message;
            }
            catch (Exception exc)
            {
                logger.Error("Failed processing UPDATE request.", exc);

                WriteResponse(string.Format("Error in UPDATE: {0}", exc.Message));
                errHandler.ErrorMessage = exc.Message;
            }
        }                
        /// DELETE function
        /// </summary>
        /// <param name="context"></param>
        private void DELETE(HttpContext context)
        {
            ILog logger = LogManager.GetLogger(GetType().FullName);
            
            try
            {
                // Queue name
                string queueName = ConfigurationManager.AppSettings["QueueName"];
                if (string.IsNullOrEmpty(queueName))
                    queueName = @".\private$\my_queue";
                logger.Debug(string.Format("QueueName=\"{0}\"", queueName));

                var msgCleaner = new QueueMessaging.MessageCleaner();
                msgCleaner.DeleteAllMessages(queueName);

                WriteResponse("OK");
            }
            catch (HttpException hexc)
            {
                logger.Error("Failed processing DELETE request.", hexc);

                WriteResponse(string.Format("Error in DELETE: {0}", hexc.Message));
                errHandler.ErrorMessage = hexc.Message;
            }
            catch (Exception exc)
            {
                logger.Error("Failed processing DELETE request.", exc);

                WriteResponse(string.Format("Error in DELETE: {0}", exc.Message));
                errHandler.ErrorMessage = exc.Message;
            }
        }

        #endregion


        #region Utility Functions
        /// <summary>
        /// Method - Writes into the Response stream
        /// </summary>
        /// <param name="strMessage"></param>
        private static void WriteResponse(string strMessage)
        {
            HttpContext.Current.Response.Write(strMessage);
        }
        /// <summary>
        /// To convert a Byte Array of Unicode values (UTF-8 encoded) to a complete String.
        /// </summary>
        /// <param name="characters">Unicode Byte Array to be converted to String</param>
        /// <returns>String converted from Unicode Byte Array</returns>
        private String UTF8ByteArrayToString(Byte[] characters)
        {
            UTF8Encoding encoding = new UTF8Encoding();
            String constructedString = encoding.GetString(characters);
            return (constructedString);
        }
        /// <summary>
        /// Method - Serialize Class to XML
        /// </summary>
        /// <param name="emp"></param>
        /// <returns></returns>
        private String Serialize(object obj)
        {
            try
            {
                String XmlizedString = null;
                XmlSerializer xs = new XmlSerializer(typeof(object));
                //create an instance of the MemoryStream class since we intend to keep the XML string 
                //in memory instead of saving it to a file.
                MemoryStream memoryStream = new MemoryStream();
                //XmlTextWriter - fast, non-cached, forward-only way of generating streams or files 
                //containing XML data
                XmlTextWriter xmlTextWriter = new XmlTextWriter(memoryStream, Encoding.UTF8);
                //Serialize emp in the xmlTextWriter
                xs.Serialize(xmlTextWriter, obj);
                //Get the BaseStream of the xmlTextWriter in the Memory Stream
                memoryStream = (MemoryStream)xmlTextWriter.BaseStream;
                //Convert to array
                XmlizedString = UTF8ByteArrayToString(memoryStream.ToArray());
                return XmlizedString;
            }
            catch (Exception ex)
            {
                errHandler.ErrorMessage = ex.Message.ToString();
                throw;
            }

        }
        #endregion
    }
}
