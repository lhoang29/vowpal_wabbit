using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace msn
{
    [ProtoContract]
    public class Header
    {
        [ProtoMember(1)]
        public float Cost;

        [ProtoMember(2)]
        public int Action;

        [ProtoMember(3)]
        public float Probability;

        [ProtoMember(4)]
        public string ChoosenDocId;
    }
}
