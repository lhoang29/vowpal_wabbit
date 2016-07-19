using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace msn
{
    public class AutoArray<T>
    {
        private T[] data = new T[0];

        private void EnsureSize(int index)
        {
            if (index >= data.Length)
            {
                var newArr = new T[index + 1];
                Array.Copy(this.data, newArr, this.data.Length);
                this.data = newArr;
            }
        }

        public T this[int index]
        {
            get
            {
                this.EnsureSize(index);
                return this.data[index];
            }

            set
            {
                this.EnsureSize(index);
                this.data[index] = value;
            }
        }

        public T[] ToArray()
        {
            return this.data;
        }
    }
}
