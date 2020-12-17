package datacloud.scala.tpobject.bintree

object BinTrees {
  // Version Int
  def contains(tree: IntTree, elem: Int): Boolean = {
    tree match {
      case EmptyIntTree => return false
      case NodeInt(e, l, r) => {
        if (e == elem) return true
        else return (contains(l, elem) || contains(r, elem))
      }
      case _ => return false
    }
  }

  def size(tree: IntTree): Int = {
    tree match {
      case EmptyIntTree => return 0
      case NodeInt(e, l, r) => {
        return 1 + size(l) + size(r)
      }
      case _ => return 0
    }
  }

  def insert(tree: IntTree, elem: Int): IntTree = {
    tree match {
      case EmptyIntTree => return new NodeInt(elem, EmptyIntTree, EmptyIntTree)
      case NodeInt(e, l, r) => {
        if (size(l) < size(r)) {
          return new NodeInt(e, insert(l, elem), r)
        } else return new NodeInt(e, l, insert(r, elem))
      }
    }
  }

  // Version generique

  def contains[A](tree: Tree[A], elem: A): Boolean = {
    tree match {
      case EmptyTree => return false
      case Node(e, l, r) => {
        if (e == elem) return true
        else return (contains(l, elem) || contains(r, elem))
      }
      case _ => return false
    }
  }

  def size[A](tree: Tree[A]): Int = {
    tree match {
      case EmptyTree => return 0
      case Node(e, l, r) => {
        return 1 + size(l) + size(r)
      }
      case _ => return 0
    }
  }

  def insert[A](tree: Tree[A], elem: A): Tree[A] = {
    tree match {
      case EmptyTree => return new Node(elem, EmptyTree, EmptyTree)
      case Node(e, l, r) => {
        if (size(l) < size(r)) return new Node(e, insert(l, elem), r)
        else return new Node(e, l, insert(r, elem))
      }
    }
  }
}