using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace msn
{
    [DataContract]
    public class Vector
    {
        [DataMember]
        public Guid ID { get; set;  }

        [DataMember]
        public float[] Data { get; set; }
    }
}
