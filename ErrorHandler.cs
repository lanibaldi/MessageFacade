using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Facade
{
    /// <summary>
    /// Class responsible for handling error messages
    /// </summary>
    public class ErrorHandler
    {
        static StringBuilder errMessage = new StringBuilder();

        /// <summary>
        /// Make class immutable
        /// </summary>
        static ErrorHandler()
        {
        }
        /// <summary>
        /// Holds exception messages encountered 
        /// at code execution
        /// </summary>
        public string ErrorMessage
        {
            get { return errMessage.ToString(); }
            set
            {
                errMessage.AppendLine(value);
            }
        }
    }
}
