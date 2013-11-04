package us.marek.cascading.viewrates;

/**
 * Helper Tuple2 class for the UserAgentRename; inspired by Scala's tuples
 * 
 * @author Marek Kolodziej
 *
 * @param <A>
 * @param <B>
 */
class Tuple2<A, B> {
	
	final A _1;
	final B _2;
	
	public Tuple2(final A _1, final B _2) {
		this._1 = _1;
		this._2 = _2;
	}

	public A _1() {
		return _1;
	}

	public B _2() {
		return _2;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((_1 == null) ? 0 : _1.hashCode());
		result = prime * result + ((_2 == null) ? 0 : _2.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Tuple2 other = (Tuple2) obj;
		if (_1 == null) {
			if (other._1 != null)
				return false;
		} else if (!_1.equals(other._1))
			return false;
		if (_2 == null) {
			if (other._2 != null)
				return false;
		} else if (!_2.equals(other._2))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Tuple2 [_1=" + _1 + ", _2=" + _2 + "]";
	}
	
}