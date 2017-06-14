package matching.ml.datamining.worldcheck

import org.apache.lucene.search.spell.StringDistance

/**
  * Class to calculate fullname similarity by combining first and last name with using weights
  *
  * Created by stefan on 5/23/17.
  */
class FullNameSimilarity(firstNameDistance: StringDistance, lastNameDistance: StringDistance, firstNameWeight: Double, lastNameWeight: Double) {
  if(firstNameWeight + lastNameWeight != 1.0){
    throw new IllegalArgumentException("weight sum need to be 1.0")
  }

  def getDistance(firstName1: String,firstName2: String, lastName1: String, lastName2: String) : Double = {
    return firstNameWeight * firstNameDistance.getDistance(firstName1,firstName2) + lastNameWeight * lastNameDistance.getDistance(lastName1,lastName2)
  }
}
