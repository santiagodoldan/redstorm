package redstorm.storm.jruby;

import storm.trident.tuple.TridentTuple;
import storm.trident.operation.BaseFilter;

import org.jruby.Ruby;
import org.jruby.RubyObject;
import org.jruby.runtime.Helpers;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.javasupport.JavaUtil;
import org.jruby.RubyModule;
import org.jruby.exceptions.RaiseException;

public class JRubyTridentFilter extends BaseFilter {
  private final String _realClassName;
  private final String _bootstrap;

  // transient to avoid serialization
  private transient IRubyObject _ruby_filter;
  private transient Ruby __ruby__;

  public JRubyTridentFilter(final String baseClassPath, final String realClassName) {
    _realClassName = realClassName;
    _bootstrap = "require '" + baseClassPath + "'";
  }

  @Override
  public boolean isKeep(final TridentTuple tuple) {
    if(_ruby_filter == null) {
      _ruby_filter = initialize_ruby_function();
    }

    IRubyObject ruby_tuple = JavaUtil.convertJavaToRuby(__ruby__, tuple);
    IRubyObject ruby_result = Helpers.invoke(__ruby__.getCurrentContext(), _ruby_filter, "keep?", ruby_tuple);
    return (boolean)ruby_result.toJava(Boolean.class);
  }

  private IRubyObject initialize_ruby_function() {
    __ruby__ = Ruby.getGlobalRuntime();

    RubyModule ruby_class;
    try {
      ruby_class = __ruby__.getClassFromPath(_realClassName);
    }
    catch (RaiseException e) {
      // after deserialization we need to recreate ruby environment
      __ruby__.evalScriptlet(_bootstrap);
      ruby_class = __ruby__.getClassFromPath(_realClassName);
    }
    return Helpers.invoke(__ruby__.getCurrentContext(), ruby_class, "new");
  }
}
